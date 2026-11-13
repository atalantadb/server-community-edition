/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell.operations.admin;

import com.light.atalanta.api.DbAdmin;
import org.jline.terminal.Terminal;

import java.util.List;

public final class GetUserPermissions {
    public static void action(DbAdmin admin, String line, String command, List<Object> argv, Terminal terminal) {
        EnterpriseNotice.show(terminal);
    }

    private GetUserPermissions() { }
}
