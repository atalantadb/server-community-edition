/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.split;

import com.google.common.base.Preconditions;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.model.RegionModel;
import com.light.atalanta.model.RegionServerModel;
import com.light.atalanta.util.Bytes;

import static com.light.atalanta.services.ZooKeeperContextHelper.SPLITTED_REGION_STATUS;

public class RegionSplitModel {
    public static final String SPLITTED_DELIMITER = "-";

    private final ZooKeeperMasterContext zooContext;
    private final RegionServerModel rs;
    private RegionDistributionModel splitting;
    private RegionDistributionModel splitted;

    public RegionSplitModel(ZooKeeperMasterContext zooContext, RegionDistributionModel splitting, RegionSplitNameGenerator nameGenerator) {
        this.zooContext = zooContext;
        this.rs = splitting.getRegionServerModel();
        this.splitting = splitting;
        this.splitted = makeSplittedRdm(nameGenerator);
    }

    public RegionSplitModel(ZooKeeperMasterContext zooContext, RegionDistributionModel splitting, RegionDistributionModel splitted) {
        this.zooContext = zooContext;
        this.rs = splitting.getRegionServerModel();
        Preconditions.checkArgument(splitting.getRegionServerModel().equals(splitted.getRegionServerModel()),
                "Splitting & splitted replicas for region: " + splitting.getRegionModel().toIdentityString() + " must be in the same region server");
        this.splitting = splitting;
        this.splitted = splitted;
    }

    public RegionDistributionModel getSplitting() {
        return splitting;
    }

    public RegionDistributionModel getSplitted() {
        return splitted;
    }

    public RegionServerModel getRs() {
        return rs;
    }

    // Returns the target name of a splitted copy based on the temporary one
    public String getTargetSplittedName() {
        String tmpSplittedName = splitted.getRegionModel().getName();
        Preconditions.checkArgument(tmpSplittedName.contains(SPLITTED_DELIMITER), "Splitted region name is already target");
        return tmpSplittedName.substring(tmpSplittedName.indexOf(SPLITTED_DELIMITER) + 1);
    }

    public void setSplittingBorders(byte[] beginKey, byte[] endKey) {
        splitting = zooContext.makeRdmCopy(splitting, splitting.getRegionServerModel(), beginKey, endKey);
    }

    public void setSplittingStatus(int status) {
        splitting = zooContext.makeRdmCopy(splitting, splitting.getRegionServerModel(), status);
    }

    public void setSplittedStatus(int status) {
        splitted = zooContext.makeRdmCopy(splitted, splitted.getRegionServerModel(), status);
    }

    public RegionDistributionModel makeSplittingRdm() {
        RegionModel original = splitting.getRegionModel();
        // Splitting region before: 000000 - ffffff
        // Splitting region after:  000000 - 800000 (inclusive=true)
        // Splitting region after:  000000 - 7fffff (inclusive=false)
        // Splitted  region after:  800000 - ffffff
        byte[][] splitKeys = Bytes.split(original.getBeginKey(), original.getEndKey(), true, 1);
        assert splitKeys != null;
        RegionModel modified = new RegionModel(original.getName(),
                original.getReplicaNum(),
                original.getStatusId(),
                splitKeys[0],
                splitKeys[1],
                original.getDataBlockSize(),
                original.getCompressType(),
                original.getRecordCompressionType(),
                original.getDictionary(),
                original.getRecordTargetCount());
        return new RegionDistributionModel(modified, splitting.getRegionServerModel());
    }

    private RegionDistributionModel makeSplittedRdm(RegionSplitNameGenerator nameGenerator) {
        RegionModel original = splitting.getRegionModel();
        // Splitting region before: 000000 - ffffff
        // Splitting region after:  000000 - 800000 (inclusive=true)
        // Splitting region after:  000000 - 7fffff (inclusive=false)
        // Splitted  region after:  800000 - ffffff
        byte[][] splitKeys = Bytes.split(original.getBeginKey(), original.getEndKey(), true, 1);
        assert splitKeys != null;
        // Temporary name of the splitted copy during the split
        String tmpSplittedName = original.getName() + SPLITTED_DELIMITER + nameGenerator.generate(original.getName());
        RegionModel modified = new RegionModel(tmpSplittedName,
                original.getReplicaNum(),
                SPLITTED_REGION_STATUS,
                splitKeys[1],
                splitKeys[2],
                original.getDataBlockSize(),
                original.getCompressType(),
                original.getRecordCompressionType(),
                original.getDictionary(),
                original.getRecordTargetCount());
        return new RegionDistributionModel(modified, splitting.getRegionServerModel());
    }

    @Override
    public String toString() {
        return String.format("%n{splitting=%s,%nsplitted=%s}", splitting, splitted);
    }
}
