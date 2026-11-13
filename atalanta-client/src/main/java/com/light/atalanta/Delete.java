/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import com.light.atalanta.api.DelRows;
import com.light.atalanta.collections.CopyOnWriteArrayMap;
import com.light.atalanta.transport.messages.MessageType;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.BytesComparator;

import java.util.*;

import static com.light.atalanta.transport.messages.MessageType.DELETE;

public class Delete implements DelRows, RowKey, RowKeyMessage {

    private final byte[] key;
    private final long timestamp;
    private NavigableMap<byte[], NavigableMap<byte[], Cell>> familyMap;

    protected Delete() {
        key = null;
        timestamp = Long.MAX_VALUE;
    }

    public Delete(byte[] key) {
        this.key = Bytes.cloneBytesArray(key);
        this.timestamp = Long.MAX_VALUE;
    }

    public Delete(byte[] key, long timestamp) {
        this.key = Bytes.cloneBytesArray(key);
        this.timestamp = timestamp;
    }

    @Override
    public DelRows addColumn(byte[] family, byte[] qualifier) {
        return addColumn(family, qualifier, timestamp);
    }

    @Override
    public DelRows addColumn(byte[] family, byte[] qualifier, long timestamp) {
        return addDeletedCell(family, qualifier, timestamp, false);
    }

    @Override
    public DelRows addColumns(byte[] family, byte[] qualifier) {
        return addColumns(family, qualifier, timestamp);
    }

    @Override
    public DelRows addColumns(byte[] family, byte[] qualifier, long timestamp) {
        return addDeletedCell(family, qualifier, timestamp, true);
    }

    private DelRows addDeletedCell(byte[] family, byte[] qualifier, long timestamp, boolean deleteColumns) {
        checkNonNullFamilyMap();
        NavigableMap<byte[], Cell> cells = familyMap.get(family);
        if (cells != null && cells.isEmpty()) {
            // A family deletion already covers this qualifier
            return this;
        }
        if (cells == null) {
            cells = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
            familyMap.put(Bytes.cloneBytesArray(family), cells);
        }
        Cell current = cells.get(qualifier);
        if (current == null || MutationCellOrder.shouldReplace(current.getTimestamp(), isDeleteColumns(current),
                timestamp, deleteColumns)) {
            QualifierValue value = new QualifierValue(timestamp, qualifier, Bytes.START_BYTES_UNIFORM);
            Cell cell = deleteColumns ? new DeleteColumnsCell(key, family, value) : new KeyValue(key, family, value);
            cells.put(cell.getQualifierArray(), cell);
        }
        return this;
    }

    static boolean isDeleteColumns(Cell cell) {
        return cell instanceof DeleteColumnsCell;
    }

    // Keep the deletion kind attached to the immutable timestamp, not in a separate
    // qualifier-only mark. This is client-side metadata; the wire format is unchanged.
    private static final class DeleteColumnsCell extends KeyValue {
        private DeleteColumnsCell(byte[] row, byte[] family, QualifierValue value) {
            super(row, family, value);
        }
    }

    @Override
    public DelRows addFamily(byte[] family, long timestamp) {
        checkNonNullFamilyMap();
        familyMap.put(family, Collections.emptyNavigableMap());
        return this;
    }

    @Override
    public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
        checkNonNullFamilyMap();
        NavigableMap<byte[], List<Cell>> cells = new CopyOnWriteArrayMap<>(BytesComparator.BYTES_COMPARATOR);
        for (Map.Entry<byte[], NavigableMap<byte[], Cell>> e : familyMap.entrySet()) {
            cells.put(e.getKey(), new ArrayList<>(e.getValue().values()));
        }
        return cells;
    }

    @Override
    public byte[] getRow() {
        return key;
    }

    @Override
    public MessageType getType() {
        return DELETE;
    }


    private void checkNonNullFamilyMap() {
        if (familyMap == null) {
            this.familyMap = new TreeMap<>(BytesComparator.BYTES_COMPARATOR);
        }
    }

}
