/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.misc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class CfgUtil {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Use YamlCfgUtil <PRM json FILE> <YAML cfg file> <Result YAML cfg file>");
            System.exit(1);
        }
        String prmFileName = args[0];
        String cfgFileName = args[1];
        String resFileName = args[2];
        File prmFile = Paths.get(prmFileName).toFile();
        File cfgFile = Paths.get(cfgFileName).toFile();
        File resFile = Paths.get(resFileName).toFile();
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> prm = objectMapper.readValue(prmFile, new TypeReference<Map<String, String>>() {});
        Map<String, Object> cfg = yamlMapper.readValue(cfgFile, new TypeReference<Map<String, Object>>() {});
        update(prm, cfg);
        yamlMapper.writeValue(resFile, cfg);
        System.out.println("Create file: " + resFile.getAbsolutePath());
    }

    private static void update(Map<String, String> prm, Map<String, Object> cfg) {
        cfg.forEach((key, value) -> {
            String v = prm.get(key);
            if (v != null) {
                cfg.put(key, v);
            } else if (value instanceof Map) {
                update(prm, (Map) value);
            }
        });
    }
}
