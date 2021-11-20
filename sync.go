package main

import (
	"context"
	"io"
	"log"

	cid "github.com/ipfs/go-cid"
)

type Client interface {
	Pins(ctx context.Context) (*cid.Set, error)
	Export(ctx context.Context, id cid.Cid) (io.ReadCloser, error)
	Import(ctx context.Context, id cid.Cid, ir io.Reader) error
}

type SyncManager struct {
	from   Client
	to     Client
	syncCh chan cid.Cid
}

func Sync(ctx context.Context, from, to Client, workers int) {
	log.Printf("starting sync process...")

	fromPins, err := from.Pins(ctx)
	if err != nil {
		log.Printf("failed to get from pins: %v", err)
		return
	}
	toPins, err := to.Pins(ctx)
	if err != nil {
		log.Printf("failed to get to pins: %v", err)
		return
	}
	sm := &SyncManager{from, to, make(chan cid.Cid)}
	for i := 0; i < workers; i++ {
		go sm.loop(ctx)
	}

	// add all pins missing that are in from but not in to
	for _, id := range fromPins.Keys() {
		if !toPins.Has(id) {
			sm.syncCh <- id
		}
	}

	log.Printf("finished sync process...")
}

func (sm *SyncManager) loop(ctx context.Context) {
	for {
		select {
		case id := <-sm.syncCh:
			sm.process(ctx, id)
		case <-ctx.Done():
			return
		}
	}
}

func (sm *SyncManager) process(ctx context.Context, id cid.Cid) {
	log.Printf("process export cid=%s", id.String())

	ir, err := sm.from.Export(ctx, id)
	if err != nil {
		log.Printf("process export failed cid=%s err=%s", id.String(), err.Error())
		return
	}
	defer ir.Close()

	log.Printf("process import cid=%s", id.String())

	if err := sm.to.Import(ctx, id, ir); err != nil {
		log.Printf("process import failed cid=%s err=%s", id.String(), err.Error())
		return
	}

	log.Printf("process success cid=%s", id.String())
}
