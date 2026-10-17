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

/*
    AtalantaDB modifications
    Copyright (C) 2021–2026 AtalantaDB Contributors
 */

package com.light.atalanta.collections;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

public class CopyOnWriteArrayMap<K, V> extends AbstractMap<K, V> implements Map<K, V>, ConcurrentNavigableMap<K, V> {
    private final Comparator<? super K> keyComparator;
    private volatile ArrayHolder<K, V> holder;

    @SuppressWarnings("unchecked")
    public CopyOnWriteArrayMap() {
        this((o1, o2) -> ((Comparable) o1).compareTo(o2));
    }

    public CopyOnWriteArrayMap(final Comparator<? super K> keyComparator) {
        this.keyComparator = keyComparator;
        this.holder = new ArrayHolder<>(keyComparator, (o1, o2) -> keyComparator.compare(o1.getKey(), o2.getKey()));
    }

    private CopyOnWriteArrayMap(final Comparator<? super K> keyComparator, ArrayHolder<K, V> holder) {
        this.keyComparator = keyComparator;
        this.holder = holder;
    }

    @Override
    public Comparator<? super K> comparator() {
        return keyComparator;
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find(fromKey);

        if (!inclusive && index >= 0) {
            index++;
        } else if (index < 0) {
            index = -(index + 1);
        }
        return new CopyOnWriteArrayMap<>(
                this.keyComparator,
                new ArrayHolder<>(
                        current.entries,
                        index,
                        current.endIndex,
                        current.keyComparator,
                        current.comparator));
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return this.tailMap(fromKey, true);
    }

    @Override
    public K firstKey() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.startIndex].getKey();
    }

    @Override
    public K lastKey() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.endIndex - 1].getKey();
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }

        int index = current.find(key);

        // There's a key exactly equal.
        if (index >= 0) {
            index -= 1;
        } else {
            index = -(index + 1) - 1;
        }

        if (index < current.startIndex || index >= current.endIndex) {
            return null;
        }
        return current.entries[index];
    }

    @Override
    public K lowerKey(K key) {
        Map.Entry<K, V> entry = lowerEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        int index = current.find(key);
        if (index < 0) {
            index = -(index + 1) - 1;
        }
        if (index < current.startIndex || index >= current.endIndex) {
            return null;
        }

        return current.entries[index];
    }

    @Override
    public K floorKey(K key) {
        Map.Entry<K, V> entry = floorEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        int index = current.find(key);
        if (index < 0) {
            index = -(index + 1);
        }
        if (index < current.startIndex || index >= current.endIndex) {
            return null;
        }

        return current.entries[index];
    }

    @Override
    public K ceilingKey(K key) {
        Map.Entry<K, V> entry = ceilingEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        int index = current.find(key);

        // There's a key exactly equal.
        if (index >= 0) {
            index += 1;
        } else {
            index = -(index + 1);
        }

        if (index < current.startIndex || index >= current.endIndex) {
            return null;
        }
        return current.entries[index];
    }

    @Override
    public K higherKey(K key) {
        Map.Entry<K, V> entry = higherEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<K, V> firstEntry() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.startIndex];
    }

    @Override
    public Entry<K, V> lastEntry() {
        ArrayHolder<K, V> current = this.holder;
        if (current.getLength() == 0) {
            return null;
        }
        return current.entries[current.endIndex - 1];
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
    public boolean containsKey(Object key) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find((K) key);
        return index >= 0;
    }

    @Override
    public V get(Object key) {

        ArrayHolder<K, V> current = this.holder;
        int index = current.find((K) key);
        if (index >= 0) {
            return current.entries[index].getValue();
        }
        return null;
    }

    @Override
    public NavigableSet<K> keySet() {
        return new ArrayKeySet<>(this, this.holder);
    }

    @Override
    public Collection<V> values() {
        return new ArrayValueCollection<>(this.holder);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new ArrayEntrySet<>(this.holder);
    }

    @Override
    public synchronized V put(K key, V value) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find(key);
        COWEntry<K, V> newEntry = new COWEntry<>(key, value);
        if (index >= 0) {
            this.holder = current.replace(index, newEntry);
            return current.entries[index].getValue();
        } else {
            this.holder = current.insert(-(index + 1), newEntry);
        }
        return null;
    }

    @Override
    public synchronized V remove(Object key) {

        ArrayHolder<K, V> current = this.holder;
        int index = current.find((K) key);
        if (index >= 0) {
            this.holder = current.remove(index);
            return current.entries[index].getValue();
        }
        return null;
    }

    @Override
    public synchronized void clear() {
        this.holder = new ArrayHolder<>(this.holder.keyComparator, this.holder.comparator);
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find(key);

        if (index < 0) {
            COWEntry<K, V> newEntry = new COWEntry<>(key, value);
            this.holder = current.insert(-(index + 1), newEntry);
            return null;
        }
        return current.entries[index].getValue();
    }

    @Override
    public synchronized boolean remove(Object key, Object value) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find((K) key);

        if (index >= 0 && current.entries[index].getValue().equals(value)) {
            this.holder = current.remove(index);
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find(key);

        if (index >= 0 && current.entries[index].getValue().equals(oldValue)) {
            COWEntry<K, V> newEntry = new COWEntry<>(key, newValue);
            this.holder = current.replace(index, newEntry);
            return true;
        }
        return false;
    }

    @Override
    public synchronized V replace(K key, V value) {
        ArrayHolder<K, V> current = this.holder;
        int index = current.find(key);

        if (index >= 0) {
            COWEntry<K, V> newEntry = new COWEntry<>(key, value);
            this.holder = current.replace(index, newEntry);
            return current.entries[index].getValue();
        }
        return null;
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap(K fromKey,
                                               boolean fromInclusive,
                                               K toKey,
                                               boolean toInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        throw new UnsupportedOperationException();
    }

}
