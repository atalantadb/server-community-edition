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
 * Adapted from Apache HBase (org.apache.hadoop.hbase.util.Bytes).
 * Modified for AtalantaDB: relocated packages, extracted classes and implementation changes.
 * See THIRD-PARTY-SOURCES.md and NOTICE for provenance and attribution.
 */

/*
    AtalantaDB modifications
    Copyright (C) 2021–2026 AtalantaDB Contributors
 */

package com.light.atalanta.util;

import java.util.Comparator;

public class BytesComparator {
    public final static Comparator<byte[]> BYTES_COMPARATOR = new ByteArrayComparator();

    /**
     * Byte array comparator class.
     */
    public static class ByteArrayComparator implements Comparator<byte[]> {

        public int compare(byte[] left, byte[] right) {
            return compareTo(left, right);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return LexicographicalComparerHolder.BEST_COMPARER.
                    compareTo(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
     * @param left  left operand
     * @param right right operand
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] left, final byte[] right) {
        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(left, 0, left.length, right, 0, right.length);
    }

    interface Comparer<T> {
        int compareTo(T buffer1, int offset1, int length1, T buffer2, int offset2, int length2);
    }

    static class LexicographicalComparerHolder {

        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();

        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java
         * implementation if unable to do so.
         */
        static Comparer<byte[]> getBestComparer() {
            return lexicographicalComparerJavaImpl();
        }

        enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                    return 0;
                }
                // Bring WritableComparator code local
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & 0xff);
                    int b = (buffer2[j] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }

                if (isArraysEmptyWithZeroBytes(buffer1, offset1, length1, buffer2, offset2, length2)) {
                    return 0;
                }

                return length1 - length2;
            }

            private boolean isArraysEmptyWithZeroBytes(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
                if (!((length1 < 8 && length2 == 8) || (length1 == 8 && length2 < 8))) {
                    return false;
                }
                int end1 = offset1 + length1;
                for (int i = offset1; i < end1; i++) {
                    if (buffer1[i] != 0) {
                        return false;
                    }
                }
                int end2 = offset2 + length2;
                for (int i = offset2; i < end2; i++) {
                    if (buffer2[i] != 0) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    private static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }

}
