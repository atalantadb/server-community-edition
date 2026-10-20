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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public final class ArrayEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    int index;
    private final ArrayHolder<K, V> holder;

    public ArrayEntryIterator(ArrayHolder<K, V> holder) {
        this.holder = holder;
        this.index = holder.startIndex;
    }

    @Override
    public boolean hasNext() {
        return index < holder.endIndex;
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IT_NO_SUCH_ELEMENT",
            justification = "Intentional")
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return holder.entries[index++];
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
