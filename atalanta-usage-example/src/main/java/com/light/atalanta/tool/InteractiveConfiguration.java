/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Locale;

/** Reads and validates the interactive data-tool settings. */
public final class InteractiveConfiguration {

    static final int DEFAULT_REGION_COUNT = 100;
    static final int LARGE_REGION_COUNT = 1000;

    private final BufferedReader input;
    private final PrintStream output;

    public InteractiveConfiguration(BufferedReader input, PrintStream output) {
        this.input = input;
        this.output = output;
    }

    public DataToolConfig read() throws IOException {
        ConnectionType connectionType = readConnectionType();
        int regionCount = readRegionCount();
        boolean recordCompression = readYesNo(
                "Compress individual records? [Y/n] ", true);
        boolean qualifierDictionary = readYesNo(
                "Enable qualifier dictionary compression "
                        + "(only when total qualifier size is below 60 KB)? [Y/n] ", true);
        long recordCount = readPositiveLong(
                "How many keys should be written and read? (try 10000000 if you have free space): ",
                DataWriter.DEFAULT_RECORD_COUNT);
        int writerThreadCount = readPositiveInteger(
                "How many worker threads should be used for writing, reading, "
                        + "and scanning? (100): ", 100);
        boolean batchWrites = readYesNo(
                "Use batch writes (100 records per region in each batch)? [Y/n] ", true);
        boolean countRecords = readYesNo(
                "Count the records in the table after writing? [Y/n] ", true);
        boolean readRecords = readYesNo(
                "Read all written records after writing? [Y/n] ", true);
        boolean scanRecords = readYesNo(
                "Scan all data after writing? [Y/n] ", true);

        return new DataToolConfig(
                connectionType, regionCount, recordCompression, qualifierDictionary,
                recordCount, writerThreadCount, batchWrites, countRecords,
                readRecords, scanRecords);
    }

    private ConnectionType readConnectionType() throws IOException {
        while (true) {
            String answer = prompt("Connection type [NIO/LocalThread] (NIO): ");
            if (answer.isEmpty() || "nio".equalsIgnoreCase(answer)) {
                return ConnectionType.NIO;
            }
            String normalized = answer.replace("-", "")
                    .replace("_", "")
                    .replace(" ", "");
            if ("localthread".equalsIgnoreCase(normalized)) {
                return ConnectionType.LOCAL_THREAD;
            }
            output.println("Please enter NIO or LocalThread.");
        }
    }

    private int readRegionCount() throws IOException {
        while (true) {
            String answer = prompt("How many table regions should be created? (100): ");
            int regionCount;
            if (answer.isEmpty()) {
                regionCount = DEFAULT_REGION_COUNT;
            } else {
                try {
                    regionCount = Integer.parseInt(answer);
                } catch (NumberFormatException e) {
                    output.println("Please enter a positive whole number.");
                    continue;
                }
            }

            if (regionCount <= 0) {
                output.println("The region count must be greater than zero.");
                continue;
            }
            if (regionCount > LARGE_REGION_COUNT) {
                boolean confirmed = readYesNo(
                        "Are you sure? Creating more than 1,000 regions may take a long time "
                                + "in some cases. [y/N] ", false);
                if (!confirmed) {
                    output.println("Please choose the number of regions again.");
                    continue;
                }
            }
            return regionCount;
        }
    }

    private int readPositiveInteger(String message, int defaultValue) throws IOException {
        while (true) {
            String answer = prompt(message);
            if (answer.isEmpty()) {
                return defaultValue;
            }
            try {
                int value = Integer.parseInt(answer);
                if (value > 0) {
                    return value;
                }
            } catch (NumberFormatException ignored) {
                // Print the same validation message for all invalid values
            }
            output.println("Please enter a positive whole number.");
        }
    }

    private long readPositiveLong(String message, long defaultValue) throws IOException {
        while (true) {
            String answer = prompt(message);
            if (answer.isEmpty()) {
                return defaultValue;
            }
            try {
                long value = Long.parseLong(answer);
                if (value > 0) {
                    return value;
                }
            } catch (NumberFormatException ignored) {
                // Print the same validation message for all invalid values
            }
            output.println("Please enter a positive whole number.");
        }
    }

    private boolean readYesNo(String message, boolean defaultValue) throws IOException {
        while (true) {
            String answer = prompt(message).toLowerCase(Locale.ROOT);
            if (answer.isEmpty()) {
                return defaultValue;
            }
            if ("y".equals(answer) || "yes".equals(answer)) {
                return true;
            }
            if ("n".equals(answer) || "no".equals(answer)) {
                return false;
            }
            output.println("Please answer yes or no.");
        }
    }

    private String prompt(String message) throws IOException {
        output.print(message);
        output.flush();
        String answer = input.readLine();
        return answer == null ? "" : answer.trim();
    }
}
