/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations;

import com.light.atalanta.exceptions.NamespaceAlreadyExistsException;
import com.light.atalanta.exceptions.NoRetryException;
import com.light.atalanta.services.ZookeeperHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.light.atalanta.ATALANTAConstants.EMPTY_BYTES;

/**
 * WARNING: Must be called by MasterImpl only.
 * TODO: Take a lock once dropping namespaces is supported.
 */
public class CreateNamespace {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateNamespace.class);

    private final ZookeeperHelper zooHelper;

    public CreateNamespace(ZookeeperHelper zooHelper) {
        this.zooHelper = zooHelper;
    }

    public void action(String namespace) throws Exception {
        LOGGER.info("Start creating namespace: '{}'", namespace);

        try {
            // Check whether the namespace exists
            boolean namespaceExists;
            try {
                namespaceExists = zooHelper.checkExists(namespace);
            } catch (Exception ex) {
                throw new Exception("unable to check namespace exists", ex);
            }
            if (namespaceExists) {
                throw new NamespaceAlreadyExistsException(namespace);
            }
            // Create the namespace
            try {
                zooHelper.create(namespace, EMPTY_BYTES);
                LOGGER.info("Done creating namespace: '{}'", namespace);
            } catch (Exception e) {
                throw new Exception("unable to create ZooKeeper namespace node", e);
            }
        } catch (Exception ex) {
            throw new NoRetryException("Error creating namespace '" + namespace + "'", ex);
        }
    }
}
