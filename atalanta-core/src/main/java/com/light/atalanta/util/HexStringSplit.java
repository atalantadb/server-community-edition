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
 * Adapted from Apache HBase (org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit).
 * Modified for AtalantaDB: relocated packages, extracted classes and implementation changes.
 * See THIRD-PARTY-SOURCES.md and NOTICE for provenance and attribution.
 */

/*
    AtalantaDB modifications
    Copyright (C) 2021–2026 AtalantaDB Contributors
 */

package com.light.atalanta.util;

import com.google.common.base.Preconditions;
import com.light.atalanta.util.Bytes;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;

public class HexStringSplit {

    private String firstRow;

    private BigInteger firstRowInt;

    private String lastRow;

    private BigInteger lastRowInt;

    private int rowComparisonLength;

    private int radix;

    public HexStringSplit() {
        this.firstRow = "00000000";
        this.lastRow = "FFFFFFFF";
        this.radix = 16;
        this.firstRowInt = BigInteger.ZERO;
        this.lastRowInt = new BigInteger(lastRow, this.radix);
        this.rowComparisonLength = lastRow.length();
    }

    public byte[] split(byte[] start, byte[] end) {
        BigInteger s = convertToBigInteger(start);
        BigInteger e = convertToBigInteger(end);
        Preconditions.checkArgument(!e.equals(BigInteger.ZERO));
        return convertToByte(split2(s, e));
    }

    public byte[][] split(int n) {
        Preconditions.checkArgument(lastRowInt.compareTo(firstRowInt) > 0,
                "last row (%s) is configured less than first row (%s)", lastRow,
                firstRow);
        // +1 to range because the last row is inclusive
        BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
        Preconditions.checkState(range.compareTo(BigInteger.valueOf(n)) >= 0,
                "split granularity (%s) is greater than the range (%s)", n, range);

        BigInteger[] splits = new BigInteger[n - 1];
        BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(n));
        for (int i = 1; i < n; i++) {
            // NOTE: this means the last region gets all the slop.
            // This is not a big deal if we're assuming n << MAXHEX
            splits[i - 1] = firstRowInt.add(sizeOfEachSplit.multiply(BigInteger
                    .valueOf(i)));
        }
        return convertToBytes(splits);
    }

    public byte[][] split(byte[] start, byte[] end, int numSplits, boolean inclusive) {
        BigInteger s = convertToBigInteger(start);
        BigInteger e = convertToBigInteger(end);

        Preconditions.checkArgument(e.compareTo(s) > 0,
                "last row (%s) is configured less than first row (%s)", rowToStr(end),
                end);
        // +1 to range because the last row is inclusive
        BigInteger range = e.subtract(s).add(BigInteger.ONE);
        Preconditions.checkState(range.compareTo(BigInteger.valueOf(numSplits)) >= 0,
                "split granularity (%s) is greater than the range (%s)", numSplits, range);

        BigInteger[] splits = new BigInteger[numSplits - 1];
        BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(numSplits));
        for (int i = 1; i < numSplits; i++) {
            // NOTE: this means the last region gets all the slop.
            // This is not a big deal if we're assuming n << MAXHEX
            splits[i - 1] = s.add(sizeOfEachSplit.multiply(BigInteger
                    .valueOf(i)));
        }

        if (inclusive) {
            BigInteger[] inclusiveSplitPoints = new BigInteger[numSplits + 1];
            inclusiveSplitPoints[0] = convertToBigInteger(start);
            inclusiveSplitPoints[numSplits] = convertToBigInteger(end);
            System.arraycopy(splits, 0, inclusiveSplitPoints, 1, splits.length);
            return convertToBytes(inclusiveSplitPoints);
        } else {
            return convertToBytes(splits);
        }
    }

    public byte[] firstRow() {
        return convertToByte(firstRowInt);
    }

    public byte[] lastRow() {
        return convertToByte(lastRowInt);
    }

    public void setFirstRow(String userInput) {
        firstRow = userInput;
        firstRowInt = new BigInteger(firstRow, radix);
    }

    public void setLastRow(String userInput) {
        lastRow = userInput;
        lastRowInt = new BigInteger(lastRow, radix);
        // Precondition: lastRow > firstRow, so last's length is the greater
        rowComparisonLength = lastRow.length();
    }

    public byte[] strToRow(String in) {
        return convertToByte(new BigInteger(in, radix));
    }

    public String rowToStr(byte[] row) {
        return Bytes.toStringBinary(row);
    }

    public String separator() {
        return " ";
    }

    public void setFirstRow(byte[] userInput) {
        firstRow = Bytes.toString(userInput);
    }

    public void setLastRow(byte[] userInput) {
        lastRow = Bytes.toString(userInput);
    }

    /**
     * Divide 2 numbers in half (for split algorithm)
     *
     * @param a number #1
     * @param b number #2
     * @return the midpoint of the 2 numbers
     */
    public BigInteger split2(BigInteger a, BigInteger b) {
        return a.add(b).divide(BigInteger.valueOf(2)).abs();
    }

    /**
     * Returns an array of bytes corresponding to an array of BigIntegers
     *
     * @param bigIntegers numbers to convert
     * @return bytes corresponding to the bigIntegers
     */
    public byte[][] convertToBytes(BigInteger[] bigIntegers) {
        byte[][] returnBytes = new byte[bigIntegers.length][];
        for (int i = 0; i < bigIntegers.length; i++) {
            returnBytes[i] = convertToByte(bigIntegers[i]);
        }
        return returnBytes;
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @param pad padding length
     * @return byte corresponding to input BigInteger
     */
    public byte[] convertToByte(BigInteger bigInteger, int pad) {
        String bigIntegerString = bigInteger.toString(radix);
        bigIntegerString = StringUtils.leftPad(bigIntegerString, pad, '0');
        return Bytes.toBytes(bigIntegerString);
    }

    /**
     * Returns the bytes corresponding to the BigInteger
     *
     * @param bigInteger number to convert
     * @return corresponding bytes
     */
    public byte[] convertToByte(BigInteger bigInteger) {
        return convertToByte(bigInteger, rowComparisonLength);
    }

    /**
     * Returns the BigInteger represented by the byte array
     *
     * @param row byte array representing row
     * @return the corresponding BigInteger
     */
    public BigInteger convertToBigInteger(byte[] row) {
        return (row.length > 0) ? new BigInteger(Bytes.toString(row), radix)
                : BigInteger.ZERO;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
                + "," + rowToStr(lastRow()) + "]";
    }
}
