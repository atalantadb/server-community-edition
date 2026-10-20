/*
 * Copyright (C) 2021–2026 AtalantaDB Contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * See LICENSE in the project root for the full license text.
 */

package com.light.atalanta.master.services.recovery;

import com.light.atalanta.model.RegionDistributionModel;

public final class RegionRecoveryModel {
    private final InitialRecoveryState initialRecoveryState;
    private RegionDistributionModel lostRdm;
    private RegionDistributionModel recoveryRdm;
    private RegionDistributionModel referenceRdm;
    private RegionDistributionModel obsoleteReferenceRdm;
    private RegionDistributionModel obsoleteRecoveryRdm;
    private boolean suddenlyAlive;

    public RegionRecoveryModel(RegionDistributionModel lostRdm, RegionDistributionModel referenceRdm, RegionDistributionModel recoveryRdm, InitialRecoveryState initialRecoveryState) {
        this.lostRdm = lostRdm;
        this.referenceRdm = referenceRdm;
        this.recoveryRdm = recoveryRdm;
        this.initialRecoveryState = initialRecoveryState;
        this.obsoleteReferenceRdm = null;
        this.obsoleteRecoveryRdm = null;
        this.suddenlyAlive = false;
    }

    public boolean isSuddenlyAlive() {
        return suddenlyAlive;
    }

    public void setSuddenlyAlive(boolean suddenlyAlive) {
        this.suddenlyAlive = suddenlyAlive;
    }

    public RegionDistributionModel getLostRdm() {
        return lostRdm;
    }

    public RegionDistributionModel getReferenceRdm() {
        return referenceRdm;
    }

    public void setLostRdm(RegionDistributionModel lostRdm) {
        this.lostRdm = lostRdm;
    }

    public void setReferenceRdm(RegionDistributionModel referenceRdm) {
        this.referenceRdm = referenceRdm;
    }

    public RegionDistributionModel getRecoveryRdm() {
        return recoveryRdm;
    }

    public void setRecoveryRdm(RegionDistributionModel recoveryRdm) {
        this.recoveryRdm = recoveryRdm;
    }

    public InitialRecoveryState getInitialRecoveryState() {
        return initialRecoveryState;
    }

    public RegionDistributionModel getObsoleteReferenceRdm() {
        return obsoleteReferenceRdm;
    }

    public void setObsoleteReferenceRdm(RegionDistributionModel obsoleteRdm) {
        this.obsoleteReferenceRdm = obsoleteRdm;
    }

    public RegionDistributionModel getObsoleteRecoveryRdm() {
        return obsoleteRecoveryRdm;
    }

    public void setObsoleteRecoveryRdm(RegionDistributionModel obsoleteRdm) {
        this.obsoleteRecoveryRdm = obsoleteRdm;
    }

    @Override
    public String toString() {
        return String.format("%n{lost=%s,%nreference=%s,%nrecovery=%s,%nstate=%s,suddenlyAlive=%s}",
                lostRdm, referenceRdm, recoveryRdm, initialRecoveryState, suddenlyAlive);
    }
}