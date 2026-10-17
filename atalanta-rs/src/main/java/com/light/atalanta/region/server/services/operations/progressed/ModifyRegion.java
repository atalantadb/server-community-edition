/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.region.server.services.operations.progressed;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.light.atalanta.TableName;
import com.light.atalanta.exceptions.ZKException;
import com.light.atalanta.model.CompressType;
import com.light.atalanta.model.RecordCompressionType;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.StorageParam;
import com.light.atalanta.region.server.data.files.*;
import com.light.atalanta.region.server.services.FileSystem;
import com.light.atalanta.region.server.services.MasterConnector;
import com.light.atalanta.region.server.services.RegionServerInfoSupplier;
import com.light.atalanta.region.server.services.ReplicaMapperProcessor;
import com.light.atalanta.region.server.vo.RegionServerConfiguration;
import com.light.atalanta.services.ZooKeeperContextHelper;
import com.light.atalanta.services.ZooKeeperCoreContext;
import com.light.atalanta.services.ZookeeperHelper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.light.atalanta.ATALANTAConstants.*;
import static com.light.atalanta.region.server.services.operations.progressed.RegionModifyStatus.*;
import static com.light.atalanta.region.server.services.operations.progressed.SplitRegion1.LOB_FILE_NAME;

public class ModifyRegion {
    private static final Logger LOGGER = LoggerFactory.getLogger(ModifyRegion.class);
    private final AtomicInteger threadIndex = new AtomicInteger(0);
    private final ZooKeeperCoreContext zooKeeperCoreContext;
    private final ZookeeperHelper zooHelper;
    private final RegionServerConfiguration conf;
    private final RegionServerInfoSupplier regionServerInfoSupplier;
    private final ReplicaMapperProcessor replicaMapperProcessor;
    private final DisableRegion disableRegion;
    private final DistributeRegion distributeRegion;
    private Thread thread;
    private final Semaphore semaphore = new Semaphore(1);
    private final ForkJoinPool threadPool;
    private final MasterConnector masterConnector;

    public ModifyRegion(ZooKeeperCoreContext zooKeeperCoreContext, ZookeeperHelper zooHelper,
                        RegionServerConfiguration conf, RegionServerInfoSupplier regionServerInfoSupplier,
                        ReplicaMapperProcessor replicaMapperProcessor, FileSystem fileSystem, MasterConnector masterConnector) {
        this.zooKeeperCoreContext = zooKeeperCoreContext;
        this.zooHelper = zooHelper;
        this.conf = conf;
        this.regionServerInfoSupplier = regionServerInfoSupplier;
        this.replicaMapperProcessor = replicaMapperProcessor;
        this.masterConnector = masterConnector;
        this.disableRegion = new DisableRegion(conf, replicaMapperProcessor);
        this.distributeRegion = new DistributeRegion(conf, replicaMapperProcessor, fileSystem);
        this.threadPool = createForkJoinPoolForModification(conf);
    }

    public void startMonitoring() {
        if (thread != null && thread.isAlive()) {
            LOGGER.warn("Modification monitoring already started in RS.");
            return;
        }
        thread = new Thread(this::monitoringProcess, "atalanta-modify-monitoring");
        thread.setDaemon(true);
        thread.start();
        LOGGER.info("Modification monitoring thread started in RS.");
    }

    private void monitoringProcess() {
        while (true) {
            try {
                if (zooHelper.checkExists(ATALANTA_MODIFY)) {
                    List<String> nsList = zooHelper.getChildren(ATALANTA_MODIFY);
                    LOGGER.info("modifying nsList size: {}", nsList.size());
                    for (String ns : nsList) {
                        processingModificationForNs(ns);
                    }
                }
                semaphore.acquire();
                if (semaphore.tryAcquire(conf.getDataConfiguration().getModifyPullTimeoutMs(), TimeUnit.MILLISECONDS) ) {
                    LOGGER.info("New modification...");
                }
                semaphore.release();
            } catch (InterruptedException e) {
                LOGGER.info("Interrupted!", e);
                throw new RuntimeException(e);
            } catch (ZKException e) {
                LOGGER.error("ZK error!", e);
                sleep();
            } catch (Exception e) {
                LOGGER.error("Some error!", e);
                sleep();
            }
        }
    }

    private ForkJoinPool createForkJoinPoolForModification(RegionServerConfiguration conf) {
        return new ForkJoinPool(conf.getDataConfiguration().getModifyPoolSize(),
                pool -> {
                    ForkJoinWorkerThread res = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    res.setName("atalanta-rs-modify-pool-" + threadIndex.incrementAndGet());
                    return res;
                }, null, false);
    }

    private void processingModificationForNs(String ns) throws ZKException, JsonProcessingException {
        String curNsPath = ATALANTA_MODIFY + ZK_SEPARATOR + ns;
        List<String> tableList = zooHelper.getChildren(curNsPath);
        LOGGER.info("Modifying ns: {} table count: {}", ns, tableList.size());
        for (String table : tableList) {
            processingModificationForTable(ns, curNsPath, table);
        }
    }

    private void processingModificationForTable(String ns, String curNsPath, String table) throws JsonProcessingException, ZKException {
        String modifyTablePath = curNsPath + ZK_SEPARATOR + table;
        StorageParam storageParam = StorageParam.fromYaml(zooHelper.get(modifyTablePath)
                .orElseThrow( () -> new RuntimeException("Unknown storage format for path: " + modifyTablePath)));
        List<String> replicasList = zooHelper.getChildren(modifyTablePath);
        LOGGER.info("Modifying table: {}:{} replicas count: {}", ns, table, replicasList.size());
        List<RegionModel> myReplicas = getMyReplicas(ns, table, replicasList);
        LOGGER.info("Modifying table: {}:{} my replicas count: {}", ns, table, myReplicas.size());
        threadPool.submit(() -> myReplicas.parallelStream().forEach(
                replica -> processingModificationForReplica(ns, table, replica, modifyTablePath, storageParam))
        ).join();
    }

    private void sleep() {
        try {
            Thread.sleep(conf.getDataConfiguration().getModifyPullTimeoutMs());
        } catch (InterruptedException ex) {
            LOGGER.info("Interrupted!", ex);
            throw new RuntimeException(ex);
        }
    }

    private void processingModificationForReplica(String ns, String table, RegionModel rm, String modifyTablePath, StorageParam storageParam) {

        LOGGER.info("Start or continue modification for {}:{} replica {}. New param: {}", ns, table, rm.toShortString(), storageParam);
        try {
            int replicaNum = rm.getReplicaNum();
            String modifyReplicaPath = modifyTablePath + ZK_SEPARATOR + rm.getName() + "_" + replicaNum;
            RegionModifyStatus regionModifyStatus = getRegionModifyStatus(modifyReplicaPath);
            String srcRegionDir = replicaMapperProcessor.getReplicaDir(rm);
            switch (regionModifyStatus) {
                case NEW:
                case MODIFYING:
                    disableTable(ns, table, rm);
                    createNewDataFile(ns, table, rm, storageParam, srcRegionDir);
                    setRegionModifyStatus(modifyReplicaPath, READY_TO_BACKUP);
                case READY_TO_BACKUP:
                    createBackup(srcRegionDir, rm, storageParam);
                    setRegionModifyStatus(modifyReplicaPath, READY_TO_COPY);
                case READY_TO_COPY:
                    updateDataFile(srcRegionDir, rm, storageParam);
                    deleteBackup(srcRegionDir, rm, storageParam);
                    setRegionModifyStatus(modifyReplicaPath, READY_TO_ENABLED);
                case READY_TO_ENABLED:
                    deleteZnode(modifyReplicaPath);
                    notifyMaster();
                    break;
                default:
                    throw new RuntimeException("Unknown modify region status: " + regionModifyStatus);
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot modify region!", ex);
            throw new RuntimeException(ex);
        }
        LOGGER.info("Complete modification for {}:{} replica {}. New param: {}", ns, table, rm.toShortString(), storageParam);
    }

    private void notifyMaster() {
        masterConnector.modificationCompleteNotification();
    }

    private void deleteBackup(String srcRegionDir, RegionModel rm, StorageParam storageParam) throws IOException {
        String bkpRegionDir = getBackupDir(srcRegionDir);
        LOGGER.info("Delete backup data: {} backupDir: {}", srcRegionDir, bkpRegionDir);
        if ( storageParam.compareFormatWithRegion(rm)) {
            LOGGER.info("Delete backup complete. No need delete backup");
            return;
        }
        FileUtils.deleteDirectory(new File(bkpRegionDir));
        LOGGER.info("Delete backup complete: {} backupDir: {}", srcRegionDir, bkpRegionDir);
    }

    private void updateDataFile(String srcRegionDir, RegionModel rm, StorageParam storageParam) {
        File newDataDir = new File(getNewDataDir(srcRegionDir));
        LOGGER.info("Start update data: {}, newDataDir: {}", srcRegionDir, newDataDir);
        if ( storageParam.compareFormatWithRegion(rm)) {
            LOGGER.info("Update data complete. No need update");
            return;
        }
        if (newDataDir.exists()) {
            if ( !newDataDir.renameTo(new File(srcRegionDir)) ) {
                throw new RuntimeException("Cannot rename region dir: " + srcRegionDir);
            }

        }
        LOGGER.info("Update data complete: {}, newDataDir: {}", srcRegionDir, newDataDir);
    }

    private void createBackup(String srcRegionDir, RegionModel rm, StorageParam storageParam) throws IOException {
        String bkpRegionDir = getBackupDir(srcRegionDir);
        LOGGER.info("Create backup for: {}, backupDir: {}", srcRegionDir, bkpRegionDir);
        if ( storageParam.compareFormatWithRegion(rm)) {
            LOGGER.info("Create backup complete. No need backup");
            return;
        }
        FileUtils.deleteDirectory(new File(bkpRegionDir));
        if (!(new File(srcRegionDir).renameTo(new File(bkpRegionDir))))  {
            throw new RuntimeException("Cannot rename dir: " + bkpRegionDir);
        }
        LOGGER.info("Create backup complete: {}, backupDir: {}", srcRegionDir, bkpRegionDir);
    }

    private static String getBackupDir(String srcRegionDir) {
        return srcRegionDir + "_bkp";
    }

    private void createNewDataFile(String ns, String table, RegionModel rm, StorageParam storageParam, String srcRegionDir) throws Exception {
        LOGGER.info("Start creating new data file. Replica: {}:{} {}_{}. Format: {}",
                ns, table, rm.getName(), rm.getReplicaNum(), storageParam);
        if (storageParam.compareFormatWithRegion(rm)) {
            if ( !storageParam.compareWithRegion(rm)) {
                RegionModel.writeRegionInfoFile(srcRegionDir, storageParam);
                LOGGER.info("Complete creating new data file. Modify only regioninfo. Replica: {}:{} {}_{}. Format: {}, srcRegionDir {}",
                        ns, table, rm.getName(), rm.getReplicaNum(), storageParam, srcRegionDir);
            } else {
                LOGGER.info("Complete creating new data file. No need modification. Replica: {}:{} {}_{}. Format: {}",
                        ns, table, rm.getName(), rm.getReplicaNum(), storageParam);
            }
            return;
        }
        String targetPath = mkNewDataDir(srcRegionDir);
        List<String> replicasDataFileList = replicaMapperProcessor.getReplicaFiles(rm);
        String srcLobFilePath = srcRegionDir + "/" + LOB_FILE_NAME;
        Boolean isLobPresent = AtalantaLobChunk.isLobNotEmpty(srcLobFilePath);
        RegionModel.writeRegionInfoFile(targetPath, storageParam);
        
        if (storageParam.getCompressType() == CompressType.none) {
            createUncompressedFiles(rm, targetPath, replicasDataFileList, srcLobFilePath, isLobPresent,
                    storageParam.getRecordCompressionType());
        } else {
            String srcFile015 = srcRegionDir + "/" + "015.data";
            String targetFile = targetPath + "/" + "015.data";
            try(AtalantaReader atalantaReader015 = createAtalantaReader(rm, srcLobFilePath, isLobPresent, srcFile015);
                AtalantaFileWriter atalantaFileWriter = new AtalantaCompressFileWriter(targetFile, atalantaReader015.getHeader(),
                    conf.getDataConfiguration().getTryToCompressLevel(), storageParam.getCompressType(), isLobPresent,
                        srcLobFilePath,targetPath, storageParam.getRecordCompressionType())) {
                for (String srcFile : replicasDataFileList) {
                    LOGGER.info("Start compress file {}. Target file: {}", srcFile, targetFile);
                    int recordCounter = 0;
                    try (AtalantaReader atalantaReader = createAtalantaReader(rm, srcLobFilePath, isLobPresent, srcFile)) {
                        for (AtalantaRecord record = atalantaReader.next(); record != null; record = atalantaReader.next()) {
                            atalantaFileWriter.write(record);
                            ++recordCounter;
                        }
                    }
                    LOGGER.info("Complete compress file {}. Target file: {}. Records: {}", srcFile, targetPath, recordCounter);
                }
            }
        }
        LOGGER.info("Complete creating new data file. Replica: {}:{} {}_{}. Format: {}",
                ns, table, rm.getName(), rm.getReplicaNum(), storageParam);
    }

    private void createUncompressedFiles(RegionModel rm, String targetPath, List<String> replicasDataFileList,
                                         String srcLobFilePath, Boolean isLobPresent,
                                         RecordCompressionType recordCompressType) throws IOException {
        if (replicasDataFileList.isEmpty()) {
            return;
        }
        String dictFile = replicasDataFileList.stream()
                .filter( f -> f.contains("015"))
                .findAny()
                .orElse(replicasDataFileList.get(0));
        try (AtalantaReader atalantaReader000 = createAtalantaReader(rm, srcLobFilePath, isLobPresent, dictFile);
             AtalantaFileWriter atalantaFileWriter = new AtalantaUncompressedFileWriter(targetPath, atalantaReader000.getHeader(),
                     isLobPresent, srcLobFilePath, recordCompressType)) {
            for (String srcFile : replicasDataFileList) {
                String srcFileName = Paths.get(srcFile).getFileName().toString();
                String targetFile = targetPath + "/" + srcFileName;
                LOGGER.info("Start uncompress file {}. Target file: {}", srcFile, targetFile);
                int recordCounter = 0;
                try (AtalantaReader atalantaReader = createAtalantaReader(rm, srcLobFilePath, isLobPresent, srcFile)) {
                    for (AtalantaRecord record = atalantaReader.next(); record != null; record = atalantaReader.next()) {
                        atalantaFileWriter.write(record);
                        ++recordCounter;
                    }
                }
                LOGGER.info("Complete uncompress file {}. Target file: {}. Records: {}", srcFile, targetPath, recordCounter);
            }
        }
    }

    private AtalantaReader createAtalantaReader(RegionModel rm, String srcLobFilePath, Boolean isLobPresent, String srcFile) throws IOException {
        return isLobPresent ?
                new AtalantaReader(srcFile, srcLobFilePath, rm.getCompressType())
                : new AtalantaReader(srcFile, rm.getCompressType());
    }

    private String mkNewDataDir(String srcRegionDir) {
        String newDataDir = getNewDataDir(srcRegionDir);
        File file = new File(newDataDir);
        if (!(file.exists()) && !file.mkdirs()) {
            throw new RuntimeException("Cannot make dir: " + newDataDir);
        }
        return newDataDir;
    }

    private static String getNewDataDir(String srcRegionDir) {
        return srcRegionDir + "_modify";
    }

    private void enableTable(String ns, String table, RegionModel rm) throws Exception {
        LOGGER.info("Enable region: {}:{} {}_{}", ns, table, rm.getName(), rm.getReplicaNum());
        AtomicReference<String> taskProgress = new AtomicReference<>();
        distributeRegion.progressedAction(TableName.valueOf(ns, table), rm, taskProgress,
                new AtomicBoolean(), StringUtils.EMPTY);
    }

    private void disableTable(String ns, String table, RegionModel rm) throws Exception {
        LOGGER.info("Disable region: {}:{} {}_{}", ns, table, rm.getName(), rm.getReplicaNum());
        AtomicReference<String> taskProgress = new AtomicReference<>();
        disableRegion.progressedAction(TableName.valueOf(ns, table), rm, taskProgress,
                new AtomicBoolean(), StringUtils.EMPTY);
    }

    private void deleteZnode(String modifyReplicaPath) throws ZKException {
        LOGGER.info("Modification complete. delete znode: {}", modifyReplicaPath);
        zooHelper.delete(modifyReplicaPath);
    }

    private void setRegionModifyStatus(String modifyReplicaPath, RegionModifyStatus regionModifyStatus) throws ZKException {
        LOGGER.info("New modification status: {}: {}", modifyReplicaPath, regionModifyStatus);
        zooHelper.setData(modifyReplicaPath, regionModifyStatus.name());
    }

    private RegionModifyStatus getRegionModifyStatus(String modifyTablePath) throws ZKException {
        String modifyTablePathStatus = modifyTablePath + ZK_SEPARATOR + regionServerInfoSupplier.regionServerId();
        if (zooHelper.checkExists(modifyTablePathStatus)) {
            return RegionModifyStatus.valueOf(zooHelper.get(modifyTablePathStatus)
                    .orElseThrow( () -> new RuntimeException("Unknown region status for path: " + modifyTablePathStatus)));
        } else {
            zooHelper.create(modifyTablePathStatus, MODIFYING.name());
            zooHelper.setData(modifyTablePath, MODIFYING.name());
            return MODIFYING;
        }
    }

    private List<RegionModel> getMyReplicas(String ns, String table, List<String> replicasList) throws ZKException {
        String path = String.join(ZK_SEPARATOR, ns, table,
                regionServerInfoSupplier.regionServerId(), ATALANTA_REGIONS_INFO);
        String regions = zooHelper.checkExists(path) ? zooHelper.get(path).orElse(StringUtils.EMPTY)
                : StringUtils.EMPTY;
        TableName tableName = TableName.valueOf(ns, table);
        String[] regionArray = ZooKeeperContextHelper.splitCsvByLines(regions);
        List<RegionModel> myReplicas = Arrays.stream(regionArray)
                .map(RegionModel::fromBase64String)
                .map(rm -> zooKeeperCoreContext.addExtParam(tableName, rm))
                .filter(rm -> replicasList.contains(rm.getName() + "_" + rm.getReplicaNum()))
                .collect(Collectors.toList());
        return zooKeeperCoreContext.addExtParam(tableName, myReplicas);
    }

    public void newModifyNotification() {
        LOGGER.info("New modify notification");
        semaphore.release();
    }
}
