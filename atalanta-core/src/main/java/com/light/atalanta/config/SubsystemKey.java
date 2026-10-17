/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.config;

import java.util.StringJoiner;

/**
 * Links settings from the client configuration to a subsystem.
 */
public class SubsystemKey {
    static final String SUBSYSTEM_PROP = "subsystem";

    private final String key;
    private final String path;
    private final String subsystemName;
    private final String subsystemKey;

    public SubsystemKey(String key) {
        if (!isSubsystemKey(key)) {
            throw new RuntimeException("Not a subsystem key: " + key);
        }
        this.key = key;
        this.path = findPath(key);

        this.subsystemName = findSubsystemName(key);
        this.subsystemKey = findSubsystemKey(key);
    }

    private String findPath(String key) {
        String[] names = key.split("\\.");
        int index = getSubsystemIndex(key);
        if (index == -1 || index >= names.length - 1) {
            throw new RuntimeException("subsystem not exists in key: " + key);
        }
        StringJoiner sj = new StringJoiner(".");
        for (int i = 0; i < index - 1; i++) {
            sj.add(names[i]);
        }
        return sj.toString();
    }

    private String findSubsystemKey(String key) {
        String[] names = key.split("\\.");
        int index = getSubsystemIndex(key);
        if (index == -1 || index >= names.length - 1) {
            throw new RuntimeException("subsystem not exists in key: " + key);
        }
        StringJoiner sj = new StringJoiner(".");
        for (int i = index + 1; i < names.length; i++) {
            sj.add(names[i]);
        }
        return sj.toString();
    }

    private String findSubsystemName(String key) {
        String[] names = key.split("\\.");
        int index = getSubsystemIndex(key);
        if (index == -1 || index >= names.length - 1) {
            throw new RuntimeException("subsystem not exists in key: " + key);
        }
        return names[index - 1];
    }

    private int getSubsystemIndex(String key) {
        String[] names = key.split("\\.");
        int index = -1;
        for (String n : names) {
            index++;
            if (n.equals(SUBSYSTEM_PROP)) {
                break;
            }
        }
        return index;
    }

    public String getKey() {
        return key;
    }

    public String getPath() {
        return path;
    }

    public String getSubsystemName() {
        return subsystemName;
    }

    public String getSubsystemKey() {
        return subsystemKey;
    }

    public static boolean isSubsystemKey(String str) {
        return str.contains("." + SUBSYSTEM_PROP + ".");
    }
}
