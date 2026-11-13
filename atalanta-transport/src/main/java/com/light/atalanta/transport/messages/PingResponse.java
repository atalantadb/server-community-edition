/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import com.light.atalanta.NumbersByteHelper;
import com.light.atalanta.exceptions.AtalantaTransportRuntimeException;
import com.light.atalanta.metrics.RegionMetrics;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Response to a ping. The core sends:
 * <pre>
 * typedef struct msg_stat_t {
 *     uint32_t msg_size;  // Size of the whole message
 *     uint8_t type;       // Type MSG_T_STAT = 14
 *     uint64_t ts_stat;   // Send timestamp
 *     char* full_stat;    // <region>_<replica>:<keys>:<mem_usage_bytes>:<count_get>:<count_put>:<count_del>;...
 * }
 * </pre>
 * Example metrics:
 * region_name00001:10:1024:1000:2000:500;region_name00002:20:2048:1100:2200:550;
 */
public class PingResponse implements TransportMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(PingResponse.class);

    private final int msgSize;

    private final long timestamp;

    private final List<RegionMetrics> regionMetrics;

    public PingResponse(byte[] fromBytes) {
        msgSize = OperationHelper.getMessageSize(fromBytes);
        MessageType transportMessageType = OperationHelper.getTransportMessageType(fromBytes);
        if (transportMessageType != MessageType.PING) {
            throw new AtalantaTransportRuntimeException("Wrong ping message type: " + transportMessageType
                    + ".Ping message type should be " + MessageType.PING.getTypeByte());
        }
        String metricString = new String(fromBytes, 5 + 8, msgSize - 5 - 8, StandardCharsets.UTF_8);
        timestamp = NumbersByteHelper.fromBytes_long(fromBytes, 5);
        LOGGER.debug("timestamp: {}, core metric string: {}", timestamp, metricString);
        regionMetrics = Arrays.stream(metricString.split(";"))
                .map(RegionMetrics::new)
                .collect(Collectors.toList());
    }

    @Override
    public ByteBuffer serializeMessage(ByteBuffer byteBuffer) throws IOException {
        throw new IllegalStateException("Method is not implemented.");
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int getMessageSize() {
        return msgSize;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.PING;
    }

    public List<RegionMetrics> getRegionMetrics() {
        return regionMetrics;
    }

    @Override
    public String toString() {
        return "PingResponse[" +
                "msgSize=" + msgSize +
                ", timestamp=" + timestamp +
                ", regionMetrics=" + regionMetrics +
                ']';
    }

}
