package sync

import (
	"context"
	"fmt"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func (s *Service) cellSubscriber(ctx context.Context, msg proto.Message) error {
	// Check message type
	vrc, ok := msg.(blocks.VerifiedROCell)
	if !ok {
		return fmt.Errorf("message was not type blocks.VerifiedROCell, type=%T", msg)
	}

	// Prevent dup / Store
	if err := s.receiveCell(ctx, vrc); err != nil {
		return err
	}

	// No logic construction yet

	return nil
}

func (s *Service) receiveCell(ctx context.Context, vrc blocks.VerifiedROCell) error {
	txHash := vrc.TxHash
	blobIndex := vrc.BlobIndex
	columnIndex := vrc.ColumnIndex

	s.setSeenCellIndex(txHash, blobIndex, columnIndex)

	if err := s.cfg.chain.ReceiveCell(ctx, vrc); err != nil {
		return errors.Wrap(err, "receive cell")
	}
	// Removed event notifier

	return nil
}
