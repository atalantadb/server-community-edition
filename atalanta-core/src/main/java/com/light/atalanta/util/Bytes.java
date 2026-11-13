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

import com.light.atalanta.NumbersByteHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

public final class Bytes {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bytes.class);

    private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

    public static final byte xFF = (byte) 0xFF;

    public static byte[] START_BYTES_UNIFORM = new byte[0];
    public static byte[] END_BYTES_UNIFORM = new byte[]{xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};

    public static byte[] START_BYTES_HEX = "00000000".getBytes();
    public static byte[] END_BYTES_HEX = "FFFFFFFF".getBytes();

    private static final char[] HEX_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    private static final char[] HEX_CHARS_UPPER = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    public static String toHex(byte[] bytes) {
        int numChars = bytes.length * 2;
        char[] ch = new char[numChars];
        for (int i = 0; i < numChars; i += 2) {
            byte d = bytes[i / 2];
            ch[i] = HEX_CHARS[(d >> 4) & 0x0F];
            ch[i + 1] = HEX_CHARS[d & 0x0F];
        }
        return new String(ch);
    }

    public static byte[] fromHex(String hex) {
        if (hex.length() % 2 == 1) {
            hex = "0" + hex;
        }
        int len = hex.length();
        byte[] b = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            b[i / 2] = hexCharsToByte(hex.charAt(i), hex.charAt(i + 1));
        }
        return b;
    }

    private static byte hexCharsToByte(char c1, char c2) {
        return (byte) ((hexCharToNibble(c1) << 4) | hexCharToNibble(c2));
    }

    private static int hexCharToNibble(char ch) {
        if (ch <= '9' && ch >= '0') {
            return ch - '0';
        } else if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        } else if (ch >= 'A' && ch <= 'F') {
            return ch - 'A' + 10;
        }
        throw new IllegalArgumentException("Invalid hex char: " + ch);
    }

    /**
     * Write a printable representation of a byte array.
     *
     * @param b byte array
     * @return string
     * @see #toStringBinary(byte[], int, int)
     */
    public static String toStringBinary(final byte[] b) {
        if (b == null)
            return "null";
        return toStringBinary(b, 0, b.length);
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc
     *
     * @param b   array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    public static String toStringBinary(final byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        // Just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length) return result.toString();
        if (off + len > b.length) len = b.length - off;
        for (int i = off; i < off + len; ++i) {
            int ch = b[i] & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char) ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }

    public static byte[] toBytesBinary(String in) {
        // this may be bigger than we need, but let's be safe.
        byte[] b = new byte[in.length()];
        int size = 0;
        for (int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i + 1 && in.charAt(i + 1) == 'x') {
                // ok, take next 2 hex digits.
                char hd1 = in.charAt(i + 2);
                char hd2 = in.charAt(i + 3);

                // they need to be A-F0-9:
                if (!isHexDigit(hd1) ||
                        !isHexDigit(hd2)) {
                    // bogus escape code, ignore:
                    continue;
                }
                // turn hex ASCII digit -> number
                byte d = (byte) ((toBinaryFromHex((byte) hd1) << 4) + toBinaryFromHex((byte) hd2));

                b[size++] = d;
                i += 3; // skip 3
            } else {
                b[size++] = (byte) ch;
            }
        }
        // resize:
        byte[] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    private static boolean isHexDigit(char c) {
        return (c >= 'A' && c <= 'F') ||
                (c >= '0' && c <= '9');
    }

    /**
     * Takes a ASCII digit in the range A-F0-9 and returns
     * the corresponding integer/ordinal value.
     *
     * @param ch The hex digit.
     * @return The converted hex value as a byte.
     */
    public static byte toBinaryFromHex(byte ch) {
        if (ch >= 'A' && ch <= 'F')
            return (byte) ((byte) 10 + (byte) (ch - 'A'));
        // else
        return (byte) (ch - '0');
    }

    /**
     * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     *
     * @param a   Beginning of range
     * @param b   End of range
     * @param num Number of times to split range.  Pass 1 if you want to split
     *            the range in two; i.e. one split.
     * @return Array of dividing values
     */
    public static byte[][] split(final byte[] a, final byte[] b, final int num) {
        return split(a, b, false, num);
    }

    /**
     * Split passed range.  Expensive operation relatively.  Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     *
     * @param a         Beginning of range
     * @param b         End of range
     * @param inclusive Whether the end of range is prefix-inclusive or is
     *                  considered an exclusive boundary.  Automatic splits are generally exclusive
     *                  and manual splits with an explicit range utilize an inclusive end of range.
     * @param num       Number of times to split range.  Pass 1 if you want to split
     *                  the range in two; i.e. one split.
     * @return Array of dividing values
     */
    public static byte[][] split(final byte[] a, final byte[] b,
                                 boolean inclusive, final int num) {
        byte[][] ret = new byte[num + 2][];
        int i = 0;
        Iterable<byte[]> iter = iterateOnSplits(a, b, inclusive, num);
        if (iter == null)
            return null;
        for (byte[] elem : iter) {
            ret[i++] = elem;
        }
        return ret;
    }

    /**
     * Iterate over keys within the passed range, splitting at an [a,b) boundary.
     */
    public static Iterable<byte[]> iterateOnSplits(final byte[] a,
                                                   final byte[] b, final int num) {
        return iterateOnSplits(a, b, false, num);
    }

    /**
     * Iterate over keys within the passed range.
     */
    public static Iterable<byte[]> iterateOnSplits(
            final byte[] a, final byte[] b, boolean inclusive, final int num) {
        byte[] aPadded;
        byte[] bPadded;
        if (a.length < b.length) {
            aPadded = padTail(a, b.length - a.length);
            bPadded = b;
        } else if (b.length < a.length) {
            aPadded = a;
            bPadded = padTail(b, a.length - b.length);
        } else {
            aPadded = a;
            bPadded = b;
        }
        if (BytesComparator.compareTo(aPadded, bPadded) >= 0) {
            throw new IllegalArgumentException("b <= a");
        }
        if (num <= 0) {
            throw new IllegalArgumentException("num cannot be <= 0");
        }
        byte[] prependHeader = {1, 0};
        final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
        final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
        BigInteger diffBI = stopBI.subtract(startBI);
        if (inclusive) {
            diffBI = diffBI.add(BigInteger.ONE);
        }
        final BigInteger splitsBI = BigInteger.valueOf(num + 1);
        //when diffBI < splitBI, use an additional byte to increase diffBI
        if (diffBI.compareTo(splitsBI) < 0) {
            byte[] aPaddedAdditional = new byte[aPadded.length + 1];
            byte[] bPaddedAdditional = new byte[bPadded.length + 1];
            for (int i = 0; i < aPadded.length; i++) {
                aPaddedAdditional[i] = aPadded[i];
            }
            for (int j = 0; j < bPadded.length; j++) {
                bPaddedAdditional[j] = bPadded[j];
            }
            aPaddedAdditional[aPadded.length] = 0;
            bPaddedAdditional[bPadded.length] = 0;
            return iterateOnSplits(aPaddedAdditional, bPaddedAdditional, inclusive, num);
        }
        final BigInteger intervalBI;
        try {
            intervalBI = diffBI.divide(splitsBI);
        } catch (Exception e) {
            LOGGER.error("Exception caught during division", e);
            return null;
        }

        final Iterator<byte[]> iterator = new Iterator<byte[]>() {
            private int i = -1;

            @Override
            public boolean hasNext() {
                return i < num + 1;
            }

            @Override
            public byte[] next() {
                i++;
                if (i == 0) return a;
                if (i == num + 1) return b;

                BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
                byte[] padded = curBI.toByteArray();
                if (padded[1] == 0)
                    padded = tail(padded, padded.length - 2);
                else
                    padded = tail(padded, padded.length - 1);
                return padded;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };

        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return iterator;
            }
        };
    }

    /**
     * @param a      array
     * @param length amount of bytes to snarf
     * @return Last <code>length</code> bytes from <code>a</code>
     */
    public static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }

    /**
     * @param a lower half
     * @param b upper half
     * @return New array that has a in lower half and b in upper half.
     */
    public static byte[] add(final byte[] a, final byte[] b) {
        return add(a, b, EMPTY_BYTE_ARRAY);
    }

    /**
     * @param a first third
     * @param b second third
     * @param c third third
     * @return New array made from a, b and c
     */
    public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
        byte[] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    /**
     * @param arrays all the arrays to concatenate together.
     * @return New array made from the concatenation of the given arrays.
     */
    public static byte[] add(final byte[][] arrays) {
        int length = 0;
        for (int i = 0; i < arrays.length; i++) {
            length += arrays[i].length;
        }
        byte[] result = new byte[length];
        int index = 0;
        for (int i = 0; i < arrays.length; i++) {
            System.arraycopy(arrays[i], 0, result, index, arrays[i].length);
            index += arrays[i].length;
        }
        return result;
    }

    /**
     * @param a      array
     * @param length new array size
     * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
     */
    public static byte[] padTail(final byte[] a, final int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(a, padding);
    }

    public static String toString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private Bytes() {
    }

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];

        for (int i = 7; i > 0; --i) {
            b[i] = (byte) ((int) val);
            val >>>= 8;
        }

        b[0] = (byte) ((int) val);
        return b;
    }

    public static byte[] toBytesRevers(long val) {
        byte[] b = new byte[8];

        for (int i = 7; i > 0; --i) {
            b[7 - i] = (byte) ((int) val);
            val >>>= 8;
        }

        b[7] = (byte) ((int) val);
        return b;
    }

    public static byte[] cloneBytesArray(byte[] src) {
        if (src == null) {
            return null;
        }
        byte[] dest = new byte[src.length];
        System.arraycopy(src, 0, dest, 0, src.length);
        return dest;
    }

    public static byte[] alignToSize(byte[] src, int size) {
        if (size < 1) {
            throw new IllegalArgumentException("size should be greater than 0");
        }
        byte[] res = new byte[size];
        for (int i = 0; i < size; i++) {
            if (src.length > i) {
                res[i] = src[i];
            } else {
                res[i] = 0;
            }
        }
        return res;
    }

    public static String toString(final byte[] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        try {
            return new String(b, off, len, UTF8_CSN);
        } catch (UnsupportedEncodingException e) {
            // should never happen!
            throw new IllegalArgumentException("UTF8 encoding is not supported", e);
        }
    }

    public static int compareTo(byte[] a, byte[] b) {
        if (a == b) return 0;
        final int lenA = (a == null) ? 0 : a.length;
        final int lenB = (b == null) ? 0 : b.length;
        final int min = Math.min(lenA, lenB);
        for (int i = 0; i < min; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return lenA - lenB;
    }

    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0);
    }

    public static short toShort(byte[] bytes, int offset) {
        return NumbersByteHelper.fromBytes_short(bytes, offset);
    }

    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0);
    }

    public static int toInt(byte[] bytes, int offset) {
        return NumbersByteHelper.fromBytes_int(bytes, offset);
    }

    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0);
    }

    public static long toLong(byte[] bytes, int offset) {
        return NumbersByteHelper.fromBytes_long(bytes, offset);
    }
}
