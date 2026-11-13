/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.tcp;

import com.light.atalanta.TableName;
import com.light.atalanta.grpc.RegionServerProto;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.region.server.vo.GrpcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class TcpFileTransfer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpFileTransfer.class);

    public static long receiveSingleFile(GrpcConfiguration grpcConf, SocketChannel clientChannel, RegionServerProto.RegionFile regionFile, String tmpRegionDir) throws IOException {
        long lastReceivedBatchStampMs = System.currentTimeMillis();
        File outputFile = new File(tmpRegionDir, regionFile.getFileName());
        long fileSize = regionFile.getFileSize();
        long bytesTransferred = 0;
        // NIO channels don't support read timeouts too;
        // moreover, they don't respect such timeouts when they're being set on sockets;
        // therefore, we have to use non-blocking NIO with Selector
        try (Selector selector = Selector.open();
             FileOutputStream fos = new FileOutputStream(outputFile);
             FileChannel destChannel = fos.getChannel()
        ) {
            // Configure the channel to be non-blocking;
            // this is required for use with a Selector
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);

            while (bytesTransferred < fileSize) {
                // Wait for the channel to be ready for reading, with a timeout;
                // this approach reliably prevents hangs
                int readyChannels = selector.select(grpcConf.getReceiveRegionTimeoutMs());

                if (readyChannels == 0) {
                    // The select() call timed out; no data arrived in time
                    throw new SocketTimeoutException("transfer stalled: no data received for " + grpcConf.getReceiveRegionTimeoutMs() + "ms");
                }

                // We don't need to iterate keys since we only have one channel registered
                // The selector must be cleared of the selected key
                selector.selectedKeys().clear();

                // The channel is ready. Perform the non-blocking transfer.
                long transferred = destChannel.transferFrom(clientChannel, bytesTransferred, fileSize - bytesTransferred);

                if (transferred > 0) {
                    bytesTransferred += transferred;
                    lastReceivedBatchStampMs = System.currentTimeMillis();
                    continue;
                }

                if (transferred == -1) {
                    // The channel was closed by the sender
                    throw new IOException(String.format("connection closed prematurely. received: %d/%d bytes for %s", bytesTransferred, fileSize, regionFile.getFileName()));
                }

                if (transferred == 0) {
                    Thread.yield();
                    if (System.currentTimeMillis() - lastReceivedBatchStampMs > grpcConf.getReceiveRegionTimeoutMs()) {
                        throw new IOException(String.format("transfer stalled: no data received for %d ms. received: %d/%d bytes for %s", grpcConf.getReceiveRegionTimeoutMs(), bytesTransferred, fileSize, regionFile.getFileName()));
                    }
                    continue;
                }

                throw new IOException("unexpected byte value: " + transferred);
            }
        }
        return bytesTransferred;
    }

    public static void sendSingleFile(GrpcConfiguration grpcConf, TableName tableName, RegionModel regionModel, String host, int port, String regionDirectoryPath, String fileName, long expectedFileSize) {
        File file = new File(regionDirectoryPath, fileName);

        if (!file.isFile()) {
            LOGGER.error("Error sending region file '{}:{}': file not found or is not a regular file: {}/{}",
                    tableName, regionModel.toShortString(), regionDirectoryPath, file
            );
            return;
        }

        LOGGER.info("Sending region file '{}:{}': connecting to {}:{} to send fileName={}",
                tableName, regionModel.toShortString(), host, port, file
        );

        try (SocketChannel socketChannel = SocketChannel.open();
             FileInputStream fis = new FileInputStream(file);
             FileChannel sourceChannel = fis.getChannel()
        ) {
            // NIO channels don't support connection timeouts out-of-box;
            // therefore, we need to get classic Socket from the channel first
            Socket socket = socketChannel.socket();
            socket.connect(new InetSocketAddress(host, port), grpcConf.getRegionServerConnectTimeoutMs());

            LOGGER.info("Sending region file '{}:{}': connected, sending fileName={}",
                    tableName, regionModel.toShortString(), fileName
            );

            long actualFileSizeBeforeSend = file.length();
            if (actualFileSizeBeforeSend != expectedFileSize) {
                throw new IOException(String.format(
                        "file size changed before transfer for %s: expected %d bytes, actual %d bytes",
                        fileName, expectedFileSize, actualFileSizeBeforeSend
                ));
            }

            long bytesSent = 0;

            while (bytesSent < expectedFileSize) {
                long transferred = sourceChannel.transferTo(bytesSent, expectedFileSize - bytesSent, socketChannel);
                if (transferred < 0) {
                    throw new IOException(String.format(
                            "source file ended prematurely for %s: sent %d/%d bytes",
                            fileName, bytesSent, expectedFileSize
                    ));
                }
                if (transferred == 0) {
                    if (sourceChannel.size() < expectedFileSize) {
                        throw new IOException(String.format(
                                "source file became smaller during transfer for %s: sent %d/%d bytes, actual %d bytes",
                                fileName, bytesSent, expectedFileSize, sourceChannel.size()
                        ));
                    }
                    Thread.yield();
                    continue;
                }
                bytesSent += transferred;
            }

            long actualFileSizeAfterSend = file.length();
            if (bytesSent != expectedFileSize || actualFileSizeAfterSend != expectedFileSize) {
                throw new IOException(String.format(
                        "file size mismatch after transfer for %s: expected %d bytes, sent %d bytes, actual %d bytes",
                        fileName, expectedFileSize, bytesSent, actualFileSizeAfterSend
                ));
            }

            LOGGER.info("Sending region file '{}:{}': successfully sent fileName={}, bytesSent={}",
                    tableName, regionModel.toShortString(), fileName, bytesSent
            );
        } catch (Exception ex) {
            LOGGER.error("Error sending region file '{}:{}': can't send fileName={}",
                    tableName, regionModel.toShortString(), fileName, ex
            );
        }
    }
}
