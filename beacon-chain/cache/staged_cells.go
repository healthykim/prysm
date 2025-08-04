package cache

import (
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v6/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v6/crypto/hash"
	"github.com/ethereum/go-ethereum/common"
)

type cellsWithTimestamp struct {
	timestamp time.Time
	cells     []blocks.VerifiedROCell
}

type verifiedEntry struct {
	timestamp time.Time
}

// CellCache is a cache that keeps track of the prepared cell for the blob (kzg commitment hash)
type CellCache struct {
	versionedHashToCellMap map[[32]byte]cellsWithTimestamp // kzg vhash -> cell (in one blob)
	buffer                 map[[32]byte]cellsWithTimestamp // tx hash -> cells (in one tx)
	isVerified             map[[32]byte]verifiedEntry      // tx hash -> cells (in one tx)
	beingVerified          map[[32]byte]struct{}           // tx hash -> cells (in one tx)
	sync.Mutex
}

// NewCellCache returns a new cell cache
func NewCellCache() *CellCache {
	return &CellCache{
		versionedHashToCellMap: make(map[[32]byte]cellsWithTimestamp),
		buffer:                 make(map[[32]byte]cellsWithTimestamp),
		isVerified:             make(map[[32]byte]verifiedEntry),
		beingVerified:          make(map[[32]byte]struct{}),
	}
}

// IsCellAvailable checks if the column is available (as a set of cells)
func (c *CellCache) IsCellAvailable(kzgCommitments [][]byte, column uint64) []blocks.VerifiedROCell {
	c.Lock()
	defer c.Unlock()

	cells := make([]blocks.VerifiedROCell, 0)
	for _, kzgCommitment := range kzgCommitments {
		versionedHash := hash.Hash(kzgCommitment)

		cwt, ok := c.versionedHashToCellMap[versionedHash]
		if !ok {
			return nil
		}
		for _, cell := range cwt.cells {
			if cell.ColumnIndex == column {
				cells = append(cells, cell)
			}
		}
	}

	return cells
}

func (c *CellCache) Set(cell blocks.VerifiedROCell) {
	c.Lock()
	defer c.Unlock()

	c.prune()

	// If the transaction hash is already verified, store the cell immediately
	if _, ok := c.isVerified[[32]byte(cell.TxHash)]; ok {
		versionedHash := hash.Hash(cell.KzgCommitment)

		// Get existing cells or create new entry
		cwt, exists := c.versionedHashToCellMap[versionedHash]
		if !exists {
			cwt = cellsWithTimestamp{
				cells:     make([]blocks.VerifiedROCell, 0),
				timestamp: time.Now(),
			}
		}

		// Add the new cell
		cwt.cells = append(cwt.cells, cell)
		c.versionedHashToCellMap[versionedHash] = cwt
		return
	}

	// Otherwise, add it to the buffer
	var hash [32]byte
	copy(hash[:], cell.TxHash)

	cwt, exists := c.buffer[hash]
	if !exists {
		cwt = cellsWithTimestamp{
			cells:     make([]blocks.VerifiedROCell, 0),
			timestamp: time.Now(),
		}
	}

	cwt.cells = append(cwt.cells, cell)
	c.buffer[hash] = cwt
}

func (c *CellCache) MakeVerified(hashes []common.Hash, verified []bool) {
	c.Lock()
	var buffered []blocks.VerifiedROCell

	for i, hash := range hashes {
		if verified[i] {
			cells := c.buffer[hash].cells
			buffered = append(buffered, cells...)
			delete(c.buffer, hash)
			c.isVerified[hash] = verifiedEntry{
				timestamp: time.Now(),
			}
		}
		delete(c.beingVerified, hash)
	}
	c.Unlock()

	for _, cell := range buffered {
		c.Set(cell)
	}
}

func (c *CellCache) GetBuffer() []common.Hash {
	c.Lock()
	defer c.Unlock()

	hashes := make([]common.Hash, 0)
	for key := range c.buffer {
		if _, ok := c.beingVerified[key]; ok {
			continue
		}
		if c.buffer[key].timestamp.Add(time.Duration(500 * time.Millisecond)).Before(time.Now()) {
			hashes = append(hashes, key)
			c.beingVerified[key] = struct{}{}
		}
	}

	return hashes
}

// todo(helathykim): where to trigger
func (c *CellCache) prune() {
	// todo(healthykim): parameter tuning
	// prune cells and buffers staged 5 slot ahead
	deadline := time.Now().Add(-1 * time.Minute)

	for key, value := range c.versionedHashToCellMap {
		if value.timestamp.Before(deadline) {
			delete(c.versionedHashToCellMap, key)
		}
	}
	for key, value := range c.buffer {
		if value.timestamp.Before(deadline) {
			delete(c.buffer, key)
		}
	}
	for key, value := range c.isVerified {
		if value.timestamp.Before(deadline) {
			delete(c.buffer, key)
		}
	}
}
