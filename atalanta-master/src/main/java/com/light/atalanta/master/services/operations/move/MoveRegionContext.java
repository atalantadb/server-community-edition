/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.operations.move;

import com.light.atalanta.TableName;
import com.light.atalanta.master.services.zookeeper.ZooKeeperMasterContext;
import com.light.atalanta.model.RegionDistributionModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.light.atalanta.services.ZooKeeperContextHelper.ASSIGNED_REGION_STATUS;
import static com.light.atalanta.services.ZooKeeperContextHelper.UNASSIGNING_REGION_STATUS;

public class MoveRegionContext {
    private final TableName tableName;
    private final MoveRegion.RetryMode retryMode;
    private final AtomicInteger replicationFactor;

    private final List<RegionMoveProjection> moveProjections;
    private final List<RegionDistributionModel> rdms;

    private final ZooKeeperMasterContext zooContext;

    public MoveRegionContext(TableName tableName, MoveRegion.RetryMode retryMode, List<RegionMoveProjection> moveProjections, ZooKeeperMasterContext zooContext) {
        this.tableName = tableName;
        this.retryMode = retryMode;
        this.moveProjections = moveProjections;
        this.zooContext = zooContext;
        this.rdms = new ArrayList<>();
        this.replicationFactor = new AtomicInteger();
    }

    public MoveRegionContext withRdms(List<RegionDistributionModel> rdms) {
        this.rdms.clear();
        this.rdms.addAll(rdms);
        return this;
    }

    public MoveRegionContext withReplicationFactor(int replicationFactor) {
        this.replicationFactor.set(replicationFactor);
        return this;
    }

    // Main methods

    public TableName getTableName() {
        return tableName;
    }

    public MoveRegion.RetryMode getRetryMode() {
        return retryMode;
    }

    public List<RegionMoveProjection> getMoveProjections() {
        return moveProjections;
    }

    public void updateRdms() throws IOException {
        this.rdms.clear();
        this.rdms.addAll(zooContext.getRegionDistributionWithStatuses(tableName));
    }

    public List<RegionDistributionModel> getRdms() {
        return new ArrayList<>(rdms);
    }

    // Additional methods

    public boolean hasSameRegionName(RegionDistributionModel rdm) {
        return moveProjections.stream().map(RegionMoveProjection::getSource).anyMatch(
                it -> it.getRegionModel().getName().equals(rdm.getRegionModel().getName())
        );
    }

    public boolean notSource(RegionDistributionModel rdm) {
        return moveProjections.stream().noneMatch(mp -> mp.getSource().equals(rdm));
    }

    public boolean notTarget(RegionDistributionModel rdm) {
        return moveProjections.stream().noneMatch(mp -> mp.getTarget().equals(rdm));
    }

    public String computeReserved(RegionDistributionModel rdm) throws Exception {
        String reservedReplicas;
        try {
            List<RegionDistributionModel> processedRdms = rdms.stream()
                    .filter(this::notSource)
                    .collect(Collectors.toList());
            reservedReplicas = zooContext.getReservedAddresses(processedRdms, rdm.getRegionModel().getName(), replicationFactor.get());
        } catch (Exception ex) {
            throw new Exception("unable to prepare reserved replicas for: " + rdm.getRegionModel().getName(), ex);
        }
        return reservedReplicas;
    }

    public String computeReservedBefore(RegionDistributionModel rdm) throws Exception {
        String reservedReplicas;
        try {
            List<RegionDistributionModel> processedRdms = rdms.stream()
                    .filter(this::notTarget)
                    .collect(Collectors.toList());
            reservedReplicas = zooContext.getReservedAddresses(processedRdms, rdm.getRegionModel().getName(), replicationFactor.get());
        } catch (Exception ex) {
            throw new Exception("unable to prepare reserved replicas for: " + rdm.getRegionModel().getName(), ex);
        }
        return reservedReplicas;
    }

    public void reflectPreCommits() {
        moveProjections.forEach(mp -> mp.setSource(
                zooContext.makeRdmCopy(mp.getSource(), mp.getSource().getRegionServerModel(), UNASSIGNING_REGION_STATUS))
        );
    }

    public void reflectDestCommits() {
        moveProjections.forEach(mp -> mp.setTarget(
                zooContext.makeRdmCopy(mp.getTarget(), mp.getTarget().getRegionServerModel(), ASSIGNED_REGION_STATUS))
        );
    }

    public void reflectRollbackPreCommits() {
        moveProjections.forEach(mp -> mp.setSource(
                zooContext.makeRdmCopy(mp.getSource(), mp.getSource().getRegionServerModel(), ASSIGNED_REGION_STATUS))
        );
    }
}