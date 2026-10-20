/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.services;

import com.light.atalanta.exceptions.ZKException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

public interface ZookeeperHelper extends Closeable {

    String getNotNullString(String path) throws ZKException;

    Optional<String> get(String path) throws ZKException;

    boolean setData(String path, String str) throws ZKException;

    CuratorOp setDataTrans(String path, String str) throws ZKException;

    void setData(String path, byte[] payload) throws ZKException;

    void setDataNoUpdateCache(String path, byte[] payload) throws ZKException;

    void updateCache(String path);

    CuratorOp setDataTrans(String path, byte[] payload) throws ZKException;

    List<String> watchedGetChildren() throws ZKException;

    List<String> watchedGetChildren(String path) throws ZKException;

    List<String> getChildren(String path) throws ZKException;

    void create(String path, String str) throws ZKException;

    /** Create a persistent node if absent, without changing an existing node's data. */
    void createIfAbsent(String path, String str) throws ZKException;

    CuratorOp createTrans(String path, String str) throws ZKException;

    CuratorOp createTrans(String path) throws ZKException;

    void create(String path, byte[] payload) throws ZKException;

    void createEphemeral(String path, String str) throws ZKException;

    CuratorOp createTrans(String path, byte[] payload) throws ZKException;

    boolean delete(String path) throws ZKException;

    CuratorOp deleteTrans(String path) throws ZKException;

    boolean checkExists(String path) throws ZKException;

    void addZookeeperCacheInvalidationListener(String pathStartWith, ZookeeperCacheInvalidationListener listener);

    CuratorFramework getCurator();

    List<CuratorTransactionResult> forOperations(List<CuratorOp> ops) throws Exception;
    List<CuratorTransactionResult> forOperationsNoCache(List<CuratorOp> ops) throws Exception;
}
