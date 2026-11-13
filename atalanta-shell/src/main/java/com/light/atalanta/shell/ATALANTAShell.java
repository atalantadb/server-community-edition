/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.shell;

import com.light.atalanta.Configuration;
import com.light.atalanta.api.DbAdmin;
import com.light.atalanta.Connection;
import com.light.atalanta.client.connection.nio.pool.ConnectionPoolNio;
import com.light.atalanta.client.connection.pool.ConnectionPool;
import com.light.atalanta.client.factories.AtalantaDbNioClientObjectFactory;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.config.AtalantaProperties;
import com.light.atalanta.shell.operations.admin.*;
import com.light.atalanta.shell.operations.data.*;
import com.light.atalanta.shell.operations.listing.*;
import com.light.atalanta.shell.operations.status.NamespaceExists;
import com.light.atalanta.shell.operations.status.TableAssigned;
import com.light.atalanta.shell.operations.status.TableCreated;
import com.light.atalanta.shell.operations.status.TableCreating;
import com.light.atalanta.shell.operations.status.TableDisabled;
import com.light.atalanta.shell.operations.status.TableDisabling;
import com.light.atalanta.shell.operations.status.TableDropping;
import com.light.atalanta.shell.operations.status.TableEnabled;
import com.light.atalanta.shell.operations.status.TableExists;
import com.light.atalanta.shell.operations.status.TableRecovering;
import com.light.atalanta.shell.operations.status.TableSplitting;
import com.light.atalanta.util.BannerPrinter;
import com.light.atalanta.util.Bytes;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.AutosuggestionWidgets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.light.atalanta.shell.Commands.*;
import static org.jline.reader.LineReader.HISTORY_FILE;

public final class ATALANTAShell {
    private static final Logger LOGGER = LoggerFactory.getLogger(ATALANTAShell.class);

    public static final ObjectFactory FACTORY = new AtalantaDbNioClientObjectFactory();

    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("config/log4j.properties");

        LOGGER.info("Starting ATALANTA shell ... ");
        BannerPrinter.print();
        Configuration atalantaConf = Configuration.create();
        atalantaConf.addResource(new File("config/atalanta-site.xml"));

        try (Connection connection = FACTORY.connection(atalantaConf)) {
            LOGGER.info("ATALANTA shell started");
            runShell(connection);
            if (connection instanceof ConnectionPool) {
                ((ConnectionPool) connection).stop();
            } else if (connection instanceof ConnectionPoolNio){
                ((ConnectionPoolNio) connection).stop();
            }
        }

        LOGGER.info("Done");
    }

    private static void runShell(Connection connection) throws Exception {
        DbAdmin admin = connection.getAdmin();
        String prompt = "atalanta-shell> ";
        LOGGER.info("grant, revoke and user_permission are available in AtalantaDB Enterprise Edition.");
        Terminal terminal = buildTerminal();

        DefaultParser parser = new DefaultParser();
        parser.setQuoteChars(new char[]{'\''});
        parser.setEscapeChars(null);

        LineReader reader = buildReader(terminal, parser);

        AutosuggestionWidgets autosuggestionWidgets = new AutosuggestionWidgets(reader);
        autosuggestionWidgets.enable();

        while (true) {
            try {
                String line = reader.readLine(prompt);
                line = ShellUtils.normalizeArgumentSeparators(line).trim();

                if (line.equalsIgnoreCase(EXIT) || line.equalsIgnoreCase(QUIT)) {
                    break;
                }
                if (line.trim().equals(StringUtils.EMPTY)) {
                    continue;
                }

                ParsedLine pl = reader.getParser().parse(line, 0);
                String command = pl.word();
                List<String> argv = pl.words().subList(1, pl.words().size());
                argv = HbaseStyleArgs.merge(argv);
                comandAction(connection, admin, terminal, line, command, parseArgs(argv, terminal));
            } catch (UserInterruptException ignored) {
                // ignore
            } catch (EndOfFileException e) {
                break;
            }
        }
    }


    private static List<Object> parseArgs(List<String> argv, Terminal terminal) {
        return argv.stream()
                .map(String::trim)
                .map(arg -> {
                    if (arg.startsWith("\"0x")) {
                        String a = arg.replace(",", StringUtils.EMPTY);
                        return Bytes.fromHex(a.substring("\"0x".length()).replace("\"", StringUtils.EMPTY));
                    } else if (arg.startsWith("\"")) {
                        String a = arg.replace(",", StringUtils.EMPTY);
                        return Bytes.toBytesBinary(a.replace("\"", StringUtils.EMPTY));
                    } else if (arg.startsWith("{") && arg.contains("}")) {
                      return HbaseStyleArgs.parse(arg, terminal);
                    } else {
                        return arg.replace("'", StringUtils.EMPTY)
                                .replace(",", StringUtils.EMPTY);
                    }
                }).collect(Collectors.toList());
    }

    private static void comandAction(Connection connection, DbAdmin admin, Terminal terminal, String line, String command, List<Object> argv) {
        switch (command) {
            case CREATE_NAMESPACE:
                CreateNamespace.action(admin, line, command, argv, terminal);
                break;
            case CREATE_TABLE:
                CreateTable.action(admin, line, command, argv, terminal);
                break;
            case ALTER_TABLE:
                AlterTable.action(admin, line, command, argv, terminal);
                break;
            case ENABLE_TABLE:
                EnableTable.action(admin, line, command, argv, terminal);
                break;
            case DISABLE_TABLE:
                DisableTable.action(admin, line, command, argv, terminal);
                break;
            case DROP_TABLE:
                DropTable.action(admin, line, command, argv, terminal);
                break;
            case IS_EXISTS:
                TableExists.action(admin, line, command, argv, terminal);
                break;
            case IS_ENABLED:
                TableEnabled.action(admin, line, command, argv, terminal);
                break;
            case IS_DISABLED:
                TableDisabled.action(admin, line, command, argv, terminal);
                break;
            case IS_CREATING:
                TableCreating.action(admin, line, command, argv, terminal);
                break;
            case IS_ASSIGNED:
                TableAssigned.action(admin, line, command, argv, terminal);
                break;
            case IS_CREATED:
                TableCreated.action(admin, line, command, argv, terminal);
                break;
            case IS_DISABLING:
                TableDisabling.action(admin, line, command, argv, terminal);
                break;
            case IS_DROPPING:
                TableDropping.action(admin, line, command, argv, terminal);
                break;
            case IS_RECOVERING:
                TableRecovering.action(admin, line, command, argv, terminal);
                break;
            case IS_SPLITTING:
                TableSplitting.action(admin, line, command, argv, terminal);
                break;
            case IS_NAMESPACE_EXISTS:
                NamespaceExists.action(admin, line, command, argv, terminal);
                break;
            case LIST:
                ListTables.action(admin, line, command, argv, terminal);
                break;
            case LIST_NAMESPACE:
                ListNamespace.action(admin, line, command, argv, terminal);
                break;
            case LIST_NAMESPACE_TABLES:
                ListNamespaceTables.action(admin, line, command, argv, terminal);
                break;
            case LIST_REGIONS:
                ListRegions.action(admin, line, command, argv, terminal);
                break;
            case LIST_REGION_SERVERS:
                ListRegionServers.action(admin, line, command, argv, terminal);
                break;
            case PUT:
                PutRows.action(connection, line, command, argv, terminal, false);
                break;
            case PUT_BYTES:
                PutRows.action(connection, line, command, argv, terminal, true);
                break;
            case PUT_SALT:
                PutRows.action(connection, line, command, argv, terminal, false, true);
                break;
            case GET:
                GetRows.action(connection, line, command, argv, terminal, false);
                break;
            case GET_BYTES:
                GetRows.action(connection, line, command, argv, terminal, true);
                break;
            case GET_SALT:
                GetRows.action(connection, line, command, argv, terminal, false, true);
                break;
            case GET_BINARY:
                GetRows.action(connection, line, command, argv, terminal, false, false, true);
                break;
            case GET_BINARY_SALT:
                GetRows.action(connection, line, command, argv, terminal, false, true, true);
                break;
            case DELETE:
                DelRows.action(connection, line, command, argv, terminal, false);
                break;
            case DELETE_SALT:
                DelRows.action(connection, line, command, argv, terminal, false, true);
                break;
            case DELETE_BYTES:
                DelRows.action(connection, line, command, argv, terminal, true);
                break;
            case SCAN:
                ScanRows.action(connection, line, command, argv, terminal);
                break;
            case COUNT:
                Count.action(connection, line, command, argv, terminal);
                break;
            case MOVE:
                MoveRegion.action(admin, line, command, argv, terminal);
                break;
            case SPLIT:
                SplitRegion.action(admin, line, command, argv, terminal);
                break;
            case REBALANCE_REGIONS:
                RebalanceRegionsAsync.action(admin, line, command, argv, terminal);
                break;
            case REBALANCE_REPLICAS:
                RebalanceReplicasAsync.action(admin, line, command, argv, terminal);
                break;
            case STOP_REBALANCE_REPLICAS:
                StopReplicaRebalanceTasks.action(admin, line, command, argv, terminal);
                break;
            case SYNC_REPLICAS:
                SyncReplicasAsync.action(admin, line, command, argv, terminal);
                break;
            case STOP_REBALANCE_REGIONS:
                StopRegionRebalanceTasks.action(admin, line, command, argv, terminal);
                break;
            case GRANT:
                Grant.action(admin, line, command, argv, terminal);
                break;
            case REVOKE:
                Revoke.action(admin, line, command, argv, terminal);
                break;
            case USER_PERMISSION:
                GetUserPermissions.action(admin, line, command, argv, terminal);
                break;
            case SNAPSHOT:
                Snapshot.action(admin, line, command, argv, terminal);
                break;
            case CLONE_SNAPSHOT:
                CloneSnapshot.action(admin, line, command, argv, terminal);
                break;
            case DELETE_SNAPSHOT:
                DeleteSnapshot.action(admin, line, command, argv, terminal);
                break;
            case LIST_SNAPSHOTS:
                ListSnapshot.action(admin, line, command, argv, terminal);
                break;
            case SALT:
                Salt.action(connection, line, command, argv, terminal);
                break;
            case START_REGION_SERVER:
                StopStartRegionServer.action(admin, line, command, argv, terminal, true);
                break;
            case STOP_REGION_SERVER:
                StopStartRegionServer.action(admin, line, command, argv, terminal, false);
                break;
            case SHUTDOWN_RS:
                ShutdownRegionServers.action(admin, line, command, argv, terminal);
                break;
            default:
                terminal.writer().println("Unknown command!");
        }
    }

    private static Terminal buildTerminal() throws IOException {
        Terminal terminal;
        try {
            terminal = TerminalBuilder.builder()
                    .name("ATALANTA shell")
                    .encoding("UTF-8")
                    .build();
        } catch (IOException ex) {
            LOGGER.error("Failed to start ATALANTA shell", ex);
            throw ex;
        }
        return terminal;
    }

    private static LineReader buildReader(Terminal terminal, DefaultParser parser) {
        return LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(HISTORY_FILE, "hist")
                .parser(parser)
                .completer(new ArgumentCompleter(new StringsCompleter(
                        CREATE_NAMESPACE,
                        CREATE_TABLE,
                        ALTER_TABLE,
                        ENABLE_TABLE,
                        DISABLE_TABLE,
                        DROP_TABLE,
                        IS_EXISTS,
                        IS_ENABLED,
                        IS_DISABLED,
                        IS_CREATING,
                        IS_CREATED,
                        IS_ASSIGNED,
                        IS_DISABLING,
                        IS_DROPPING,
                        IS_RECOVERING,
                        IS_SPLITTING,
                        IS_NAMESPACE_EXISTS,
                        LIST,
                        LIST_NAMESPACE,
                        LIST_NAMESPACE_TABLES,
                        LIST_REGIONS,
                        LIST_REGION_SERVERS,
                        PUT,
                        PUT_BYTES,
                        GET,
                        GET_BYTES,
                        DELETE,
                        DELETE_BYTES,
                        SCAN,
                        COUNT,
                        MOVE,
                        SPLIT,
                        REBALANCE_REPLICAS,
                        STOP_REBALANCE_REPLICAS,
                        REBALANCE_REGIONS,
                        STOP_REBALANCE_REGIONS,
                        SYNC_REPLICAS,
                        GRANT,
                        REVOKE,
                        USER_PERMISSION,
                        EXIT,
                        SNAPSHOT,
                        LIST_SNAPSHOTS,
                        DELETE_SNAPSHOT,
                        CLONE_SNAPSHOT,
                        SALT,
                        GET_SALT,
                        PUT_SALT,
                        DELETE_SALT,
                        GET_BINARY,
                        GET_BINARY_SALT,
                        STOP_REGION_SERVER,
                        SHUTDOWN_RS,
                        START_REGION_SERVER)))
                .build();
    }

    private ATALANTAShell() {
    }
}
