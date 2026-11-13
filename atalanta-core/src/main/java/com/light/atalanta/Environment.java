/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class Environment {

    private static final String ENVIRONMENT_DOCKER = "docker";

    public static boolean isDockerEnvironment(final String profile) {
        return Objects.equals(ENVIRONMENT_DOCKER, profile);
    }

    public static String getLocalHost(final String profile) throws UnknownHostException {
        // Lowercase the host name: Linux may change its case, for example 'fedora' to 'Fedora'
        return (isDockerEnvironment(profile) ? "localhost" : InetAddress.getLocalHost().getCanonicalHostName().toLowerCase());
    }

    public static String getLocalAddress(final String profile, final int port) throws UnknownHostException {
        return getLocalHost(profile) + ":" + port;
    }
}
