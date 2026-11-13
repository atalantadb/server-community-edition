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

import java.util.Comparator;
import java.util.Map;

public final class ArrayHolder<K, V> {
    final COWEntry<K, V>[] entries;
    final int startIndex;
    final int endIndex;
    final Comparator<? super K> keyComparator;
    final Comparator<Map.Entry<K, V>> comparator;


    int getLength() {
        return endIndex - startIndex;
    }


    /**
     * Binary search for a given key
     *
     * @param needle The key to look for in all of the entries
     * @return Same return value as Arrays.binarySearch.
     * Positive numbers mean the index.
     * Otherwise (-1 * insertion point) - 1
     */
    int find(K needle) {
        int begin = startIndex;
        int end = endIndex - 1;

        while (begin <= end) {
            int mid = begin + ((end - begin) / 2);
            K midKey = entries[mid].key;
            int compareRes = keyComparator.compare(midKey, needle);

            // 0 means equals
            // We found the key.
            if (compareRes == 0) {
                return mid;
            } else if (compareRes < 0) {
                // midKey is less than needle so we need
                // to look at farther up
                begin = mid + 1;
            } else {
                // midKey is greater than needle so we
                // need to look down.
                end = mid - 1;
            }
        }

        return (-1 * begin) - 1;
    }

    ArrayHolder<K, V> replace(int index, COWEntry<K, V> newEntry) {
        // TODO should this restart the array back at start index 0 ?
        COWEntry<K, V>[] newEntries = entries.clone();
        newEntries[index] = newEntry;
        return new ArrayHolder<>(newEntries, startIndex, endIndex, keyComparator, comparator);
    }

    ArrayHolder<K, V> remove(int index) {
        COWEntry<K, V>[] newEntries = new COWEntry[getLength() - 1];
        System.arraycopy(this.entries, startIndex, newEntries, 0, index - startIndex);
        System.arraycopy(this.entries, index + 1, newEntries, index, entries.length - index - 1);
        return new ArrayHolder<>(newEntries, 0, newEntries.length, keyComparator, comparator);
    }

    ArrayHolder<K, V> insert(int index, COWEntry<K, V> newEntry) {
        COWEntry<K, V>[] newEntries = new COWEntry[getLength() + 1];
        System.arraycopy(this.entries, startIndex, newEntries, 0, index - startIndex);
        newEntries[index] = newEntry;
        System.arraycopy(this.entries, index, newEntries, index + 1, getLength() - index);
        return new ArrayHolder<>(newEntries, 0, newEntries.length, keyComparator, comparator);
    }

    public ArrayHolder(
            final Comparator<? super K> keyComparator,
            final Comparator<Map.Entry<K, V>> comparator) {
        this.endIndex = 0;
        this.startIndex = 0;
        this.entries = new COWEntry[]{};
        this.keyComparator = keyComparator;
        this.comparator = comparator;
    }

    public ArrayHolder(COWEntry[] entries,
                       int startIndex, int endIndex,
                       final Comparator<? super K> keyComparator,
                       Comparator<Map.Entry<K, V>> comparator) {
        this.entries = entries;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.keyComparator = keyComparator;
        this.comparator = comparator;
    }
}
