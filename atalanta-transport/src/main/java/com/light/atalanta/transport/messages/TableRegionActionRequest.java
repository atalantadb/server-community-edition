/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NameOfTable;
import com.light.atalanta.model.RegionModel;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


public class TableRegionActionRequest implements TransportMessage {

    private final byte[] serializedObject;

    private TableRegionActionRequest(byte[] serializedObject) {
        this.serializedObject = serializedObject;
    }

    // Special shutdown command
    public static TableRegionActionRequest shutdown() {
        return new TableRegionActionRequest(("::::::" + Command.SHUTDOWN.getValue()).getBytes(StandardCharsets.UTF_8));
    }

    public TableRegionActionRequest(NameOfTable tableName, RegionModel regionModel, Command cmd,
                                    String path, String reservedReplicas) {
        // namespace:table:region_name:start_key:end_key:path:cmd:replica1:port:replica2:port
        String strEncoded = String.join(":", tableName.getNamespace(), tableName.getTableName(),
                regionModel.toIdentityString(),
                Base64.getEncoder().encodeToString(regionModel.getBeginKey()),
                Base64.getEncoder().encodeToString(regionModel.getEndKey()), path, String.valueOf(cmd.getValue()));
        if (StringUtils.isNotEmpty(reservedReplicas)) {
            strEncoded += ":" + String.join(":", reservedReplicas);
        }
        this.serializedObject = strEncoded.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int getMessageSize() {
        return serializedObject.length;
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer bb) {
        return bb.put(serializedObject);
    }

    @Override
    public MessageType getMessageType() {
        return null;
    }

    public enum Command {
        UNASSIGN(0), ASSIGN(1), STOP(2), START(3), SHUTDOWN(4);

        private final int value;

        Command(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
