package sync

import (
	"context"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v7/async"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/cmd/beacon-chain/flags"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var nilFinalizedStateError = errors.New("finalized state is nil")

func (s *Service) maintainCustodyInfo() {
	const interval = 1 * time.Minute

	async.RunEvery(s.ctx, interval, func() {
		if err := s.updateCustodyInfoIfNeeded(); err != nil {
			log.WithError(err).Error("Failed to update custody info")
		}
	})
}

func (s *Service) updateCustodyInfoIfNeeded() error {
	const minimumPeerCount = 1

	// Get our actual custody group count.
	actualCustodyGrounpCount, err := s.cfg.p2p.CustodyGroupCount(s.ctx)
	if err != nil {
		return errors.Wrap(err, "p2p custody group count")
	}

	// Get our target custody group count.
	targetCustodyGroupCount, err := s.custodyGroupCount(s.ctx)
	if err != nil {
		return errors.Wrap(err, "custody group count")
	}

	log.WithFields(logrus.Fields{
		"actualCustodyGroupCount": actualCustodyGrounpCount,
		"targetCustodyGroupCount": targetCustodyGroupCount,
	}).Info("Custody info update check")

	// If the actual custody group count is already equal to the target, skip the update.
	if actualCustodyGrounpCount >= targetCustodyGroupCount {
		log.Info("Skipping custody update - already at target")
		return nil
	}

	// Check that all subscribed data column sidecars topics have at least `minimumPeerCount` peers.
	topics := s.cfg.p2p.PubSub().GetTopics()
	enoughPeers := true
	for _, topic := range topics {
		if !strings.Contains(topic, p2p.GossipDataColumnSidecarMessage) {
			continue
		}

		if peers := s.cfg.p2p.PubSub().ListPeers(topic); len(peers) < minimumPeerCount {
			// If a topic has fewer than the minimum required peers, log a warning.
			log.WithFields(logrus.Fields{
				"topic":            topic,
				"peerCount":        len(peers),
				"minimumPeerCount": minimumPeerCount,
			}).Debug("Insufficient peers for data column sidecar topic to maintain custody count")
			enoughPeers = false
		}
	}

	if !enoughPeers {
		return nil
	}

	headROBlock, err := s.cfg.chain.HeadBlock(s.ctx)
	if err != nil {
		return errors.Wrap(err, "head block")
	}
	headSlot := headROBlock.Block().Slot()

	storedEarliestSlot, storedGroupCount, err := s.cfg.p2p.UpdateCustodyInfo(headSlot, targetCustodyGroupCount)
	if err != nil {
		return errors.Wrap(err, "p2p update custody info")
	}

	if _, _, err := s.cfg.beaconDB.UpdateCustodyInfo(s.ctx, storedEarliestSlot, storedGroupCount); err != nil {
		return errors.Wrap(err, "beacon db update custody info")
	}

	log.WithFields(logrus.Fields{
		"storedGroupCount":        storedGroupCount,
		"actualCustodyGroupCount": actualCustodyGrounpCount,
		"targetCustodyGroupCount": targetCustodyGroupCount,
	}).Info("After UpdateCustodyInfo")

	// Check if we should skip custody changes due to proposer preparation
	if storedGroupCount > actualCustodyGrounpCount {
		// Check if blockchain service is preparing for proposal
		if s.cfg.chain.ShouldSkipCustodyChange(s.ctx) {
			log.WithFields(logrus.Fields{
				"storedGroupCount": storedGroupCount,
				"actualGroupCount": actualCustodyGrounpCount,
			}).Info("Skipped custody columns change due to proposer preparation")
			return nil
		}

		nodeID := s.cfg.p2p.NodeID()
		peerInfo, _, err := peerdas.Info(nodeID, storedGroupCount)
		if err != nil {
			log.WithError(err).Error("Failed to compute custody columns for notification")
		} else if peerInfo != nil && len(peerInfo.CustodyColumns) > 0 {
			if err := s.cfg.p2p.NotifyCustodyColumnsChange(s.ctx, peerInfo.CustodyColumns, s.cfg.executionReconstructor); err != nil {
				log.WithError(err).Error("Failed to notify custody columns change")
			}
		}
	}

	return nil
}

// custodyGroupCount computes the custody group count based on the custody requirement,
// the validators custody requirement, and whether the node is subscribed to all data subnets.
func (s *Service) custodyGroupCount(ctx context.Context) (uint64, error) {
	cfg := params.BeaconConfig()

	if flags.Get().SubscribeAllDataSubnets {
		return cfg.NumberOfCustodyGroups, nil
	}

	validatorsCustodyRequirement, err := s.validatorsCustodyRequirement(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "validators custody requirement")
	}

	result := max(cfg.CustodyRequirement, validatorsCustodyRequirement)
	log.WithFields(logrus.Fields{
		"custodyRequirement":           cfg.CustodyRequirement,
		"validatorsCustodyRequirement": validatorsCustodyRequirement,
		"result":                       result,
	}).Info("Custody group count calculation")

	return result, nil
}

// validatorsCustodyRequirement computes the custody requirements based on the
// head state and the tracked validators. Using head state instead of finalized
// state allows earlier detection of custody requirement changes.
func (s *Service) validatorsCustodyRequirement(ctx context.Context) (uint64, error) {
	if s.trackedValidatorsCache == nil {
		return 0, nil
	}
	// Get the indices of the tracked validators.
	indices := s.trackedValidatorsCache.Indices()

	// Return early if no validators are tracked.
	if len(indices) == 0 {
		return 0, nil
	}

	// Retrieve the head state for earlier detection of custody changes.
	headState, err := s.cfg.chain.HeadStateReadOnly(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "head state")
	}
	if headState == nil || headState.IsNil() {
		return 0, errors.New("head state is nil")
	}

	// Compute the validators custody requirements.
	result, err := peerdas.ValidatorsCustodyRequirement(headState, indices)
	if err != nil {
		return 0, errors.Wrap(err, "validators custody requirements")
	}

	return result, nil
}
