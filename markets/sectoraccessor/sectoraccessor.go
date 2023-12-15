package sectoraccessor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
)

var log = logging.Logger("sectoraccessor")

type sectorAccessor struct {
	maddr address.Address
	secb  sectorblocks.SectorBuilder
	pp    sealer.PieceProvider
	full  v1api.FullNode
}

var _ retrievalmarket.SectorAccessor = (*sectorAccessor)(nil)

func NewSectorAccessor(maddr dtypes.MinerAddress, secb sectorblocks.SectorBuilder, pp sealer.PieceProvider, full v1api.FullNode) dagstore.SectorAccessor {
	return &sectorAccessor{address.Address(maddr), secb, pp, full}
}

func (sa *sectorAccessor) UnsealSector(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (io.ReadCloser, error) {
	return sa.UnsealSectorAt(ctx, sectorID, pieceOffset, length)
}

func (sa *sectorAccessor) UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	log.Debugf("get sector %d, pieceOffset %d, length %d", sectorID, pieceOffset, length)
	si, err := sa.sectorsStatus(ctx, sectorID, false)
	if err != nil {
		return nil, err
	}

	mid, err := address.IDFromAddress(sa.maddr)
	if err != nil {
		return nil, err
	}

	ref := storiface.SectorRef{
		ID: abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: sectorID,
		},
		ProofType: si.SealProof,
	}

	var commD cid.Cid
	if si.CommD != nil {
		commD = *si.CommD
	}

	// Get a reader for the piece, unsealing the piece if necessary
	log.Debugf("read piece in sector %d, pieceOffset %d, length %d from miner %d", sectorID, pieceOffset, length, mid)
	r, unsealed, err := sa.pp.ReadPiece(ctx, ref, storiface.UnpaddedByteIndex(pieceOffset), length, si.Ticket.Value, commD)
	if err != nil {
		return nil, xerrors.Errorf("failed to unseal piece from sector %d: %w", sectorID, err)
	}
	_ = unsealed // todo: use

	return r, nil
}

func (sa *sectorAccessor) UnsealSectorAtOfSxx(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize, piececid cid.Cid) (mount.Reader, error) {
	log.Debugf("get sector %d, pieceOffset %d, length %d", sectorID, pieceOffset, length)
	defer func() {
		log.Infof("完成 UnsealSectorAtOfSxx")
	}()
	si, err := sa.sectorsStatus(ctx, sectorID, false)
	if err != nil {
		return nil, err
	}

	mid, err := address.IDFromAddress(sa.maddr)
	if err != nil {
		return nil, err
	}

	ref := storiface.SectorRef{
		ID: abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: sectorID,
		},
		ProofType: si.SealProof,
	}

	var commD cid.Cid
	if si.CommD != nil {
		commD = *si.CommD
	}

	// Get a reader for the piece, unsealing the piece if necessary
	log.Infof("read piece %+v in sector %d, pieceOffset %d, length %d from miner %d", piececid, sectorID, pieceOffset, length, mid)
	// 删减lotus中的读取方式
	// r, unsealed, err := sa.pp.ReadPieceOfSxx(ctx, ref, storiface.UnpaddedByteIndex(pieceOffset), length, si.Ticket.Value, commD, piececid)
	r, unsealed, err := ReadPieceOfSxx(ctx, ref, storiface.UnpaddedByteIndex(pieceOffset), length, si.Ticket.Value, commD, piececid)
	if err != nil {
		return nil, xerrors.Errorf("failed to unseal piece from sector %d: %w", sectorID, err)
	}
	_ = unsealed // todo: use

	return r, nil
}

func (sa *sectorAccessor) IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	si, err := sa.sectorsStatus(ctx, sectorID, true)
	if err != nil {
		return false, xerrors.Errorf("failed to get sector info: %w", err)
	}

	mid, err := address.IDFromAddress(sa.maddr)
	if err != nil {
		return false, err
	}

	ref := storiface.SectorRef{
		ID: abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: sectorID,
		},
		ProofType: si.SealProof,
	}

	log.Debugf("will call IsUnsealed now sector=%+v, offset=%d, size=%d", sectorID, offset, length)
	return sa.pp.IsUnsealed(ctx, ref, storiface.UnpaddedByteIndex(offset), length)
}

func (sa *sectorAccessor) sectorsStatus(ctx context.Context, sid abi.SectorNumber, showOnChainInfo bool) (api.SectorInfo, error) {
	sInfo, err := sa.secb.SectorsStatus(ctx, sid, false)
	if err != nil {
		return api.SectorInfo{}, err
	}

	if !showOnChainInfo {
		return sInfo, nil
	}

	onChainInfo, err := sa.full.StateSectorGetInfo(ctx, sa.maddr, sid, types.EmptyTSK)
	if err != nil {
		return sInfo, err
	}
	if onChainInfo == nil {
		return sInfo, nil
	}
	sInfo.SealProof = onChainInfo.SealProof
	sInfo.Activation = onChainInfo.Activation
	sInfo.Expiration = onChainInfo.Expiration
	sInfo.DealWeight = onChainInfo.DealWeight
	sInfo.VerifiedDealWeight = onChainInfo.VerifiedDealWeight
	sInfo.InitialPledge = onChainInfo.InitialPledge

	ex, err := sa.full.StateSectorExpiration(ctx, sa.maddr, sid, types.EmptyTSK)
	if err != nil {
		return sInfo, nil
	}
	sInfo.OnTime = ex.OnTime
	sInfo.Early = ex.Early

	return sInfo, nil
}

func ReadPieceOfSxx(ctx context.Context, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid, piececid cid.Cid) (mount.Reader, bool, error) {
	defer func() {
		log.Infof("完成 ReadPieceOfSxx")
	}()
	if err := pieceOffset.Valid(); err != nil {
		return nil, false, xerrors.Errorf("pieceOffset is not valid: %w", err)
	}
	if err := size.Validate(); err != nil {
		return nil, false, xerrors.Errorf("size is not a valid piece size: %w", err)
	}

	log.Infof("你们都会到我这里来")
	r, err := tryReadUnsealedPieceOfSxx(ctx, unsealed, sector, pieceOffset, size, piececid)

	if xerrors.Is(err, storiface.ErrSectorNotFound) {
		log.Debugf("no unsealed sector file with unsealed piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)
		err = nil
	}
	if err != nil {
		log.Errorf("returning error from ReadPiece:%s", err)
		return nil, false, err
	}

	var uns bool

	if r == nil {
		return nil, false, xerrors.Errorf("got no reader after unsealing piece")
	}

	log.Debugf("returning reader to read unsealed piece, sector=%+v, pieceOffset=%d, size=%d", sector, pieceOffset, size)

	return r, uns, nil
}

type CarDir struct {
	Miner string
	Dir   []string
}

func tryReadUnsealedPieceOfSxx(ctx context.Context, pc cid.Cid, sector storiface.SectorRef, pieceOffset storiface.UnpaddedByteIndex, pieceSize abi.UnpaddedPieceSize, piececid cid.Cid) (mount.Reader, error) {
	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)

	// 优先从现有car目录查找文件，找不到再查看是否构造
	worker_car_json_file := filepath.Join(os.Getenv("BOOST_PATH"), "./retrieve_path.json")
	_, err := os.Stat(worker_car_json_file)
	if err != nil {
		cancel()
		return nil, xerrors.Errorf("don't have json file of car path")
	}
	byteValue, err := ioutil.ReadFile(worker_car_json_file)
	if err != nil {
		cancel()
		return nil, xerrors.Errorf("can't read %+v, err: %+v", worker_car_json_file, err)
	}
	var car_dir CarDir
	json.Unmarshal(byteValue, &car_dir)

	worker_car_path := ""
	for _, curdir := range car_dir.Dir {
		gen_path := filepath.Join(curdir, piececid.String()+".car")
		_, err = os.Stat(gen_path)
		if err == nil {
			worker_car_path = gen_path
			shell := fmt.Sprintf("sudo chmod 666 %+v", worker_car_path)
			cmd := exec.Command("sh", "-c", shell)
			cmd.Run()
			break
		}
	}

	// 找不到现有car文件
	if worker_car_path == "" {
		log.Errorf("zlin : 读取不到文件： %+v", piececid.String()+".car")
		// r, err := p.tryReadUnsealedPiece(ctx, pc, sector, pieceOffset, pieceSize)
		cancel()
		return nil, err
	}

	log.Errorf("zlin : 读取文件： %+v", worker_car_path)
	content, err := os.OpenFile(worker_car_path, os.O_RDONLY, 0644)
	if err != nil {
		cancel()
		return nil, xerrors.Errorf("can't read file: %w", err)
	}
	log.Errorf("zlin : 文件完成打开： %+v", worker_car_path)

	pr, err := (&pieceReader{
		getReader: func(startOffset, readSize uint64) (io.ReadCloser, error) {

			content.Seek(int64(startOffset), io.SeekStart)

			bir := bufio.NewReaderSize(content, 127)

			return struct {
				io.Reader
				io.Closer
			}{
				Reader: bir,
				Closer: funcCloser(func() error {
					log.Errorf("zlin 文件关闭")
					return content.Close()
				}),
			}, nil
		},
		len:      pieceSize,
		onClose:  cancel,
		pieceCid: piececid,
	}).init(ctx)
	if err != nil || pr == nil { // pr == nil to make sure we don't return typed nil
		cancel()
		return nil, err
	}

	return pr, err
}

type funcCloser func() error

func (f funcCloser) Close() error {
	return f()
}

var _ io.Closer = funcCloser(nil)
