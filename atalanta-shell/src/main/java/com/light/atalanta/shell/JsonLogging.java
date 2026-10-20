/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class JsonLogging implements Closeable, AutoCloseable {

    private static ObjectMapper jsonMapper = new ObjectMapper();

    private final String pathToJsonFile;

    private Boolean firstLine = true;

    public JsonLogging(Object pathToFileWithPrefix) throws IOException {
        if (jsonLoggingEnable(pathToFileWithPrefix)) {
            pathToJsonFile = getPathToJsonFile(pathToFileWithPrefix);
            Files.write(Paths.get(getPathToJsonFile(pathToFileWithPrefix)),
                    "[".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } else {
            pathToJsonFile = null;
        }
    }

    public void log(String line, String tableName, Object result) {
        try {
            if (pathToJsonFile != null) {
                Map<String, Object> map = new HashMap<>();
                map.put("cmd", line);
                map.put("table", tableName);
                map.put("result", result);
                jsonMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
                String res = (firstLine ? "" : "," + System.lineSeparator()) + jsonMapper.writeValueAsString(map);
                firstLine = false;
                Files.write(Paths.get(pathToJsonFile),
                        res.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (pathToJsonFile != null) {
            Files.write(Paths.get(pathToJsonFile),
                    "]".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private static String getPathToJsonFile(Object pathToFileWithPrefix) {
        return ((String) pathToFileWithPrefix).substring(1);
    }

    private static boolean jsonLoggingEnable(Object pathToFile) {
        return pathToFile instanceof String
                && ((String) pathToFile).startsWith("@")
                && ((String)pathToFile).length() > 1;
    }

}
