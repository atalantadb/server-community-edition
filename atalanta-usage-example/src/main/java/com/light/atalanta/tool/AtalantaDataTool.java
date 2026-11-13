/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * Entry point for the interactive AtalantaDB usage example.
 */
public final class AtalantaDataTool {

    private static final Path DEFAULT_CONFIGURATION = Paths.get("config", "atalanta-site.xml");

    private AtalantaDataTool() {
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Configuration: " + DEFAULT_CONFIGURATION.toAbsolutePath());

        System.out.println("AtalantaDB Usage Example (press ENTER to set default)");
        if (!Files.isRegularFile(DEFAULT_CONFIGURATION)) {
            System.err.println("ERROR: Configuration file was not found: "
                    + DEFAULT_CONFIGURATION.toAbsolutePath());
            System.exit(1);
        }

        InteractiveConfiguration questionnaire = new InteractiveConfiguration(
                new BufferedReader(new InputStreamReader(System.in)), System.out);
        DataToolConfig configuration = questionnaire.read();

        System.out.println();
        System.out.println("Selected configuration:");
        System.out.println("  Connection type: " + configuration.getConnectionType().getDisplayName());
        System.out.println("  Table regions: " + configuration.getRegionCount());
        System.out.println("  Record compression: " + yesNo(configuration.isRecordCompressionEnabled()));
        System.out.println("  Qualifier dictionary compression: " + yesNo(configuration.isQualifierDictionaryRequested()));
        System.out.printf(Locale.US, "  Keys to write and read: %,d%n",  configuration.getRecordCount());
        System.out.println("  Worker threads: " + configuration.getWriterThreadCount());
        System.out.println("  Batch writes: " + yesNo(configuration.isBatchWritesEnabled()));
        if (configuration.isBatchWritesEnabled()) {
            System.out.println("  Batch size: " + configuration.getBatchSize() + " records (100 per region)");
        } else {
            System.out.println("  Write mode: single operations");
        }
        System.out.println("  Count records: " + yesNo(configuration.isCountRecordsEnabled()));
        System.out.println("  Read records: " + yesNo(configuration.isReadRecordsEnabled()));
        System.out.println("  Scan data: " + yesNo(configuration.isScanRecordsEnabled()));
        System.out.println("Configuration completed.");

        DatabaseInitializer initializer = new DatabaseInitializer();
        try {
            DatabaseInitializer.Result result = initializer.initialize(
                    DEFAULT_CONFIGURATION, configuration);
            System.out.println(result.isNamespaceCreated() ? "Namespace created: " + DataToolSchema.NAMESPACE : "Namespace already exists: " + DataToolSchema.NAMESPACE);
            System.out.println(result.isTableCreated() ? "Table created: " + DataToolSchema.qualifiedTableName()
                    : "Table already exists and was left unchanged: " + DataToolSchema.qualifiedTableName());
            System.out.println("Database initialization completed.");

            DataWriter.WriteResult writeResult = new DataWriter().write(DEFAULT_CONFIGURATION, configuration, System.out);
            System.out.println("Write completed: " + writeResult.getRecordsWritten() + " records in " + writeResult.getElapsedMillis() + " ms.");

            if (configuration.isCountRecordsEnabled()) {
                long recordCount = new DataVerifier().count(DEFAULT_CONFIGURATION, configuration);
                System.out.println("Table record count: " + recordCount);
                if (recordCount != configuration.getRecordCount()) {
                    throw new IllegalStateException("Expected table record count "
                            + configuration.getRecordCount() + ", but found " + recordCount + '.');
                }
            }

            DataReader.ReadResult readResult = null;
            if (configuration.isReadRecordsEnabled()) {
                readResult = new DataReader().read(DEFAULT_CONFIGURATION, configuration, System.out);
                System.out.println("Read completed: " + readResult.getRecordsRead() + " records in " + readResult.getElapsedMillis() + " ms.");
            }

            DataScanner.ScanResult scanResult = null;
            if (configuration.isScanRecordsEnabled()) {
                scanResult = new DataScanner().scan(DEFAULT_CONFIGURATION, configuration, System.out);
                System.out.println("Scan completed: " + scanResult.getRecordsScanned() + " records in " + scanResult.getElapsedMillis() + " ms.");
            }
            System.out.println();
            System.out.println("Performance summary:");
            System.out.printf(Locale.US, "  Write OPS: %,.2f%n", writeResult.getOperationsPerSecond());
            if (readResult != null) {
                System.out.printf(Locale.US, "  Read OPS: %,.2f%n", readResult.getOperationsPerSecond());
            } else {
                System.out.println("  Read OPS: not measured (reading was disabled)");
            }
            if (scanResult != null) {
                System.out.printf(Locale.US, "  Scan OPS: %,.2f%n", scanResult.getOperationsPerSecond());
            } else {
                System.out.println("  Scan OPS: not measured (scanning was disabled)");
            }
            System.out.println("Completed.");
        } catch (Exception e) {
            System.err.println("ERROR: Usage-example operation failed: " + e.getMessage());
            System.exit(1);
        }
    }

    private static String yesNo(boolean value) {
        return value ? "yes" : "no";
    }
}
