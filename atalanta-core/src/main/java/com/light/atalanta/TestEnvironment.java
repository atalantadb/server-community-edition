/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta;

import org.apache.commons.lang3.StringUtils;

import java.io.File;

public class TestEnvironment {
    private static Configuration atalantaConf = null;
    private static final String profile;

    static {
        profile = System.getProperty("config.folder");
        if (StringUtils.isBlank(profile)) {
            throw new IllegalArgumentException("'config.folder' parameter should be setup by active profile");
        }
    }

    public static String getProfile() {
        return profile;
    }

    public static Configuration configuration(final boolean isNewConfiguration) throws Exception {
        if (isNewConfiguration) {
            return buildConfiguration();
        }
        if (atalantaConf != null) {
            return atalantaConf;
        }

        atalantaConf = buildConfiguration();
        return atalantaConf;
    }

    private static Configuration buildConfiguration() throws Exception {
        atalantaConf = Configuration.create();
        atalantaConf.addResource(new File("src/test/resources/config/" + profile + "/atalanta-site.xml"));
        return atalantaConf;
    }

}
