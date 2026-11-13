/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

/**
 * Compatibility entry point for code ported from the HBase client API.
 * <p>
 * It only mirrors {@code HBaseConfiguration.create()} to simplify porting. New code should use
 * {@code Configuration.create()} and the AtalantaDB interfaces in {@code com.light.atalanta}.
 * <p>
 * To keep code that works with both HBase and AtalantaDB, program against the interfaces and create
 * objects through an {@code ObjectFactory} instead of constructors, for example
 * {@code FACTORY.put(rowKey)} instead of {@code new Put(rowKey)}. AtalantaDB factories:
 * <ul>
 * <li>{@code AtalantaDbConnectionPoolObjectFactory}: pooled connections, the closest to the HBase client;</li>
 * <li>{@code AtalantaDbObjectFactory}: low-level factory, not thread-safe.</li>
 * </ul>
 */
public class HBaseConfiguration extends Configuration {
    public static Configuration create() {
        return (Configuration) Configuration.create();
    }
}
