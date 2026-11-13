/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.util;

import com.light.atalanta.Result;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;

import java.nio.charset.StandardCharsets;

import static io.grpc.netty.shaded.io.netty.util.internal.StringUtil.toHexString;

final public class ClientUtil {

    private ClientUtil() {
    }

    public static String resultToString(Result result, boolean bytes) {
        StringBuilder sb = new StringBuilder();
        for (KeyValues kv : result.getKeyValues()) {
            for (QualifierValue qv : kv.getValues()) {
                String value;
                if (bytes) {
                    value = toHexString(qv.getValue());
                } else {
                    value = new String(qv.getValue(), StandardCharsets.UTF_8);
                }
                sb.append(String.format("%nkey=%s\tcf:qualifier=%s:%s, timestamp=%s, value=%s",
                        new String(kv.getRowKey(), StandardCharsets.UTF_8),
                        new String(kv.getCfName(), StandardCharsets.UTF_8),
                        new String(qv.getQualifier(), StandardCharsets.UTF_8),
                        qv.getTimestamp(),
                        value));
            }
        }
        if (sb.toString().length() > 0) {
            return sb.toString();
        }
        return "\nNo results found." + sb;
    }

}
