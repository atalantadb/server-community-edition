/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Adapted from Apache HBase (org.apache.hadoop.hbase.types.CopyOnWriteArrayMap).
 * Modified for AtalantaDB: relocated packages, extracted classes and implementation changes.
 * See THIRD-PARTY-SOURCES.md and NOTICE for provenance and attribution.
 */

package com.light.atalanta.collections;

import java.lang.reflect.Array;
import java.util.*;

public final class ArrayKeySet<K, V> implements NavigableSet<K> {

    private final CopyOnWriteArrayMap copyOnWriteArrayMap;
    private final ArrayHolder<K, V> holder;

    public ArrayKeySet(CopyOnWriteArrayMap copyOnWriteArrayMap, ArrayHolder<K, V> holder) {
        this.copyOnWriteArrayMap = copyOnWriteArrayMap;
        this.holder = holder;
    }

    @Override
    public int size() {
        return holder.getLength();
    }

    @Override
    public boolean isEmpty() {
        return holder.getLength() == 0;
    }

    @Override
    public boolean contains(Object o) {
        ArrayHolder<K, V> current = this.holder;

        for (int i = current.startIndex; i < current.endIndex; i++) {
            if (current.entries[i].getValue().equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public K lower(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K floor(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K ceiling(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K higher(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public K pollFirst() {
        throw new UnsupportedOperationException();
    }

    @Override
    public K pollLast() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<K> iterator() {
        return new ArrayKeyIterator<>(this.holder);
    }

    @Override
    public NavigableSet<K> descendingSet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<K> descendingIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> subSet(K fromElement,
                                  boolean fromInclusive,
                                  K toElement,
                                  boolean toInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> headSet(K toElement, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<? super K> comparator() {
        return (Comparator<? super K>) copyOnWriteArrayMap.comparator();
    }

    @Override
    public SortedSet<K> subSet(K fromElement, K toElement) {
        return null;
    }

    @Override
    public SortedSet<K> headSet(K toElement) {
        return null;
    }

    @Override
    public SortedSet<K> tailSet(K fromElement) {
        return null;
    }

    @Override
    public K first() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.startIndex].getKey();
    }

    @Override
    public K last() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.endIndex - 1].getKey();
    }

    @Override
    public Object[] toArray() {
        ArrayHolder<K, V> current = this.holder;
        int length = current.getLength();
        Object[] result = new Object[length];
        int out = 0;
        for (int i = current.startIndex; i < current.endIndex; i++) {
            result[out++] = current.entries[i].getKey();
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        ArrayHolder<K, V> current = this.holder;
        int length = current.getLength();
        T[] result = a.length >= length
                ? a
                : (T[]) Array.newInstance(a.getClass().getComponentType(), length);

        int out = 0;
        for (int i = current.startIndex; i < current.endIndex; i++) {
            result[out++] = (T) current.entries[i].getKey();
        }
        if (result.length > length) {
            result[length] = null;
        }
        return result;
    }

    @Override
    public boolean add(K k) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends K> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
