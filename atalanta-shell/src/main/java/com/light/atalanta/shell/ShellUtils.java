/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell;

import com.light.atalanta.Result;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.XxHash32KeySalter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class ShellUtils {

    /** Keep separator commas attached to the preceding argument; preserve quoted data. */
    public static String normalizeArgumentSeparators(String line) {
        StringBuilder normalized = new StringBuilder(line.length());
        char quote = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quote != 0) {
                normalized.append(c);
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '\'' || c == '"') {
                quote = c;
                normalized.append(c);
            } else if (c == ',') {
                while (normalized.length() > 0 && normalized.charAt(normalized.length() - 1) == ' ') {
                    normalized.setLength(normalized.length() - 1);
                }
                normalized.append(c);
                while (i + 1 < line.length() && line.charAt(i + 1) == ' ') {
                    i++;
                }
                if (i + 1 < line.length() && !Character.isWhitespace(line.charAt(i + 1))) {
                    normalized.append(' ');
                }
            } else {
                normalized.append(c);
            }
        }
        return normalized.toString();
    }

    // command 'argument'
    private static final Pattern SINGLE_ARG_PATTERN = Pattern.compile("('|\").*('|\")");

    // command 'argument1', 'argument2'
    private static final Pattern DUAL_ARG_PATTERN = Pattern.compile("('|\").*('|\")( *?),( *?)('|\").*('|\")");

    // command 'argument1', 'argument2', 'argument3'
    private static final Pattern TRIPLE_ARG_PATTERN = Pattern.compile("('|\").*('|\")( *?),( *?)('|\").*('|\")( *?),( *?)('|\").*('|\")");

    // command 'argument1', 'argument2', 'argument3', 'argument4'
    private static final Pattern QUADRUPLE_ARG_PATTERN = Pattern.compile("('|\").*('|\")( *?),( *?)('|\").*('|\")( *?),( *?)('|\").*('|\")( *?),( *?)('|\").*('|\")");

    // command 'argument1', 'argument2', 'argument3', 'argument4', 'argument5'
    private static final Pattern FIVEFOLD_ARG_PATTERN = Pattern.compile("'.*', '.*', '.*', '.*', '.*'");

    public static boolean isValidEmptyArgString(String line, String command) {
        return StringUtils.countMatches(line, "'") == 0
                && line.equals(command);
    }

    public static boolean isValidSingleArgString(String line, String command) {
        String argNew = line.replace(command, StringUtils.EMPTY).trim();
        return SINGLE_ARG_PATTERN.matcher(argNew).matches();
    }

    public static boolean isValidDualArgString(String line, String command) {
        String argNew = line.replace(command, StringUtils.EMPTY).trim();
        return DUAL_ARG_PATTERN.matcher(argNew).matches();
    }

    public static boolean isValidTripleArgString(String line, String command) {
        String argNew = line.replace(command, StringUtils.EMPTY).trim();
        return TRIPLE_ARG_PATTERN.matcher(argNew).matches();
    }

    public static boolean isValidQuadrupleArgString(String line, String command) {
        String argNew = line.replace(command, StringUtils.EMPTY).trim();
        return QUADRUPLE_ARG_PATTERN.matcher(argNew).matches();
    }

    public static boolean isValidFiveFoldArgString(String line, String command) {
        String argNew = line.replace(command, StringUtils.EMPTY).trim();
        return FIVEFOLD_ARG_PATTERN.matcher(argNew).matches();
    }

    public static String listToStringsAsc(List<?> objects) {
        String list = objects.stream().map(Object::toString)
                .sorted()
                .collect(Collectors.joining("\n"));
        if (objects.size() > 0) {
            list = list + "\n";
        }
        return list;
    }

    public static String resultToString(Result result, boolean bytes, Map<String, Map<String, String>> values) {
        StringBuilder sb = new StringBuilder();
        if (result.getKeyValues() != null) {
            for (KeyValues kv : sortCfAsc(result.getKeyValues())) {
                for (QualifierValue qv : sortQualifiersAsc(kv.getValues())) {
                    if(kv.getValues().length == 0) {
                        sb.append(String.format("%nkey=%s\tno value!",
                                Bytes.toStringBinary(kv.getRowKey())));
                    }
                    String value;
                    if (bytes) {
                        value = Hex.encodeHexString(qv.getValue());
                    } else {
                        value = new String(qv.getValue(), StandardCharsets.UTF_8);
                    }
                    String cf = new String(kv.getCfName(), StandardCharsets.UTF_8);
                    String colName = new String(qv.getQualifier(), StandardCharsets.UTF_8);
                    sb.append(String.format("%nkey=%s\tcf:qualifier=%s:%s, timestamp=%s, value=%s",
                            Bytes.toStringBinary(kv.getRowKey()),
                            cf,
                            colName,
                            qv.getTimestamp(),
                            value));
                    if (values != null) {
                        Map<String, String> row = values.computeIfAbsent(Bytes.toStringBinary(kv.getRowKey()),
                                k -> new HashMap<>());
                        row.put(cf + ":" + colName, value);

                    }
                }
            }
        }
        if (sb.toString().length() > 0) {
            return sb.toString();
        }
        return "\nNo results found." + sb;
    }

    public static Map<String, String> getPropertiesFromArguments(List<Object> args) {
        if (args == null || args.size() < 4) {
            return Collections.emptyMap();
        }
        Map<String, String> props = null;
        for (int i = 0; i < args.size(); i++) {
            String s = (String) args.get(i);
            if (StringUtils.equals("=>", s)) {
                if (i == 0 || i == 1 || i == args.size() - 1) {
                    throw new RuntimeException("Incorrect properties usage");
                }
                String name = ((String) args.get(i - 1)).toUpperCase();
                String value = (String) args.get(i + 1);
                if (props == null) {
                    props = new HashMap<>();
                }
                props.put(name, value);
            }
        }
        return props == null ? Collections.emptyMap() : props;
    }

    public static byte[] getByteValue(Object v, boolean salt) {
       byte[] res = v instanceof byte[] ? (byte[])v : ((String) v).getBytes();
       return salt ? XxHash32KeySalter.saltKey(res) : res;
    }

    public static List<KeyValues> sortCfAsc(KeyValues[] keys) {
        return Arrays.stream(keys)
                .sorted(Comparator.comparing(q -> new String(q.getCfName(), StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
    }

    public static List<QualifierValue> sortQualifiersAsc(QualifierValue[] values) {
        return Arrays.stream(values)
                .sorted(Comparator.comparing(q -> new String(q.getQualifier(), StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
    }

    private ShellUtils() {
    }
}
