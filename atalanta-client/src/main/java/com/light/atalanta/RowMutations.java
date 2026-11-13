/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DbRowMutations;
import com.light.atalanta.api.PutRows;
import com.light.atalanta.client.MutationValidator;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.util.BytesComparator;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RowMutations implements DbRowMutations, RowKeyMessage {
    private final byte[] rowkey;
    private NavigableMap<byte[], NavigableMap<byte[], Pair<Cell, Type>>> familyMap = null;

    private boolean isFullDelete = false;

    public RowMutations(byte[] rowkey) {
        MutationValidator.validateBytes(rowkey);
        this.rowkey = rowkey;
    }

    List<Pair<Cell, Type>> getCells() {
        checkNonNullFamilyMap();
        List<Pair<Cell, Type>> results = new ArrayList<>();
        familyMap.values().forEach(m -> m.forEach((k, v) -> results.add(v)));
        return results;
    }

    @Override
    public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
        checkNonNullFamilyMap();
        return familyMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> new ArrayList<>(), (o, n) -> {
            o.addAll(n);
            return o;
        }, () -> new TreeMap<>(BytesComparator.BYTES_COMPARATOR)));
    }

    @Override
    public byte[] getRow() {
        return rowkey;
    }

    @Override
    public void add(Mutation mutation) throws IOException {
        if (isFullDelete) {
            return;
        }
        if (mutation instanceof Delete && mutation.getFamilyCellMap().isEmpty()) {
            isFullDelete = true;
        } else {
            checkNonNullFamilyMap();
            for (Map.Entry<byte[], List<Cell>> e : mutation.getFamilyCellMap().entrySet()) {
                if (mutation instanceof Delete && e.getValue().isEmpty()) {
                    // Deleting a whole column family is not fully supported yet, so the entire row is deleted
                    isFullDelete = true;
                    break;
                }
                this.familyMap.putIfAbsent(e.getKey(), new TreeMap<>(BytesComparator.BYTES_COMPARATOR));
                NavigableMap<byte[], Pair<Cell, Type>> cells = this.familyMap.get(e.getKey());
                for (Cell c : e.getValue()) {
                    Type incomingType = resolveType(mutation, c);
                    Pair<Cell, Type> pair = cells.get(c.getQualifierArray());
                    if (pair == null || MutationCellOrder.shouldReplace(
                            pair.getFirst().getTimestamp(), pair.getSecond() == Type.DELETE_COLUMNS,
                            c.getTimestamp(), incomingType == Type.DELETE_COLUMNS)) {
                        cells.put(c.getQualifierArray(), new Pair<>(c, incomingType));
                    }
                }
            }
        }
    }

    private static Type resolveType(Mutation mutation, Cell cell) {
        if (mutation instanceof PutRows) {
            return Type.PUT;
        }
        if (mutation instanceof Delete && Delete.isDeleteColumns(cell)) {
            return Type.DELETE_COLUMNS;
        }
        return Type.DELETE;
    }

    private void checkNonNullFamilyMap() {
        if (this.familyMap == null) {
            this.familyMap = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        }
    }

    public boolean isFullDelete() {
        return isFullDelete;
    }

    @Override
    public MessageType getType() {
        return MessageType.PUT;
    }

    public NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> getFamilyMap() {
        checkNonNullFamilyMap();
        NavigableMap<byte[], NavigableMap<byte[], Pair<Long, byte[]>>> results = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        for (byte[] b : getFamilyCellMap().keySet()) {
            results.putIfAbsent(b, new TreeMap<>(BytesComparator.BYTES_COMPARATOR));
            NavigableMap<byte[], Pair<Long, byte[]>> values = results.get(b);
            for (Cell c : getFamilyCellMap().get(b)) {
                values.put(c.cloneQualifier(), new Pair<>(c.getTimestamp(), c.getValueArray()));
            }
        }
        return results;
    }

    enum Type {
        PUT, DELETE, DELETE_COLUMNS
    }
}
