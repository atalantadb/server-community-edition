/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.transport.messages;

import java.util.List;

public interface BulkOperation extends PositionedTransportMessage {
    List<? extends Operation> getOperations();
}
