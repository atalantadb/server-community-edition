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
 * Adapted from Apache HBase (org.apache.hadoop.hbase.util.RegionSplitter.UniformSplit).
 * Modified for AtalantaDB: relocated packages, extracted classes and implementation changes.
 * See THIRD-PARTY-SOURCES.md and NOTICE for provenance and attribution.
 */

/*
    AtalantaDB modifications
    Copyright (C) 2021–2026 AtalantaDB Contributors
 */

package com.light.atalanta.util;

import com.light.atalanta.Preconditions;
import java.util.Arrays;

import static com.light.atalanta.util.Bytes.END_BYTES_UNIFORM;
import static com.light.atalanta.util.Bytes.START_BYTES_UNIFORM;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

public class UniformSplit {

    static final byte xFF = (byte) 0xFF;

    public byte[] split(byte[] start, byte[] end) {
      return Bytes.split(start, end, 1)[1];
    }

    public byte[][] split(int numRegions) {
      Preconditions.checkArgument(
          BytesComparator.compareTo(END_BYTES_UNIFORM, START_BYTES_UNIFORM) > 0,
          "last row (%s) is configured less than first row (%s)",
          Bytes.toStringBinary(END_BYTES_UNIFORM),
          Bytes.toStringBinary(START_BYTES_UNIFORM));

      byte[][] splits = Bytes.split(START_BYTES_UNIFORM, END_BYTES_UNIFORM, true,
          numRegions - 1);
      Preconditions.checkState(splits != null,
          "Could not split region with given user input: " + this);

      // remove endpoints, which are included in the splits list

      return Arrays.copyOfRange(splits, 1, splits.length - 1);
    }

    public byte[][] split(byte[] start, byte[] end, int numSplits, boolean inclusive) {
      if (Arrays.equals(start, EMPTY_BYTE_ARRAY)) {
        start = START_BYTES_UNIFORM;
      }
      if (Arrays.equals(end, EMPTY_BYTE_ARRAY)) {
        end = END_BYTES_UNIFORM;
      }
      Preconditions.checkArgument(
              BytesComparator.compareTo(end, start) > 0,
              "last row (%s) is configured less than first row (%s)",
              Bytes.toStringBinary(end),
              Bytes.toStringBinary(start));

      byte[][] splits = Bytes.split(start, end, true,
              numSplits - 1);
      Preconditions.checkState(splits != null,
              "Could not calculate input splits with given user input: " + this);
      if (inclusive) {
        return splits;
      } else {
        // remove endpoints, which are included in the splits list
        return Arrays.copyOfRange(splits, 1, splits.length - 1);
      }
    }

    public byte[] firstRow() {
      return START_BYTES_UNIFORM;
    }

    public byte[] lastRow() {
      return END_BYTES_UNIFORM;
    }

    public void setFirstRow(byte[] userInput) {
      START_BYTES_UNIFORM = userInput;
    }

    public void setLastRow(byte[] userInput) {
      END_BYTES_UNIFORM = userInput;
    }

    public String rowToStr(byte[] row) {
      return Bytes.toStringBinary(row);
    }

    public String separator() {
      return ",";
    }

    public String toString() {
        return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
                + "," + rowToStr(lastRow()) + "]";
    }
  }
