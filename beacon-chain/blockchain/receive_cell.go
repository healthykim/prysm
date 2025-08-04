package blockchain

import (
	"context"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
)

// ReceiveDataColumn receives a single data column.
// (It is only a wrapper around ReceiveDataColumns.)
func (s *Service) ReceiveCell(ctx context.Context, vrc blocks.VerifiedROCell) error {
	s.stagedCellCache.Set(vrc)

	hashes := s.stagedCellCache.GetBuffer()
	if len(hashes) > 0 {
		exists, err := s.cfg.ExecutionEngineCaller.CellVerification(ctx, hashes)
		if err != nil {
			return err
		}

		for i, hash := range hashes {
			if exists[i] {
				log.Info("Verified", "hash", hash)
			}
		}

		s.stagedCellCache.MakeVerified(hashes, exists)
	}

	return nil
}
