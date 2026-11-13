/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import org.jline.terminal.Terminal;

/** Edition notice only: never performs an administrative operation. */
public final class EnterpriseNotice {
    public static final String MESSAGE = "This feature is available in AtalantaDB Enterprise Edition.";

    public static void show(Terminal terminal) {
        terminal.writer().println(MESSAGE);
        terminal.writer().flush();
    }

    private EnterpriseNotice() { }
}
