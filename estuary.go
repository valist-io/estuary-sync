package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	cid "github.com/ipfs/go-cid"
	httppin "github.com/ipfs/go-pinning-service-http-client"
)

type EstuaryClient struct {
	http *http.Client
	pins *httppin.Client
	url  string
	key  string
}

func NewEstuaryClient(url, key string) (*EstuaryClient, error) {
	if url == "" {
		return nil, fmt.Errorf("estuary url is required")
	}
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	return &EstuaryClient{
		http: client,
		pins: httppin.NewClient(url+"/pinning", key),
		url:  url,
		key:  key,
	}, nil
}

func (c *EstuaryClient) Pins(ctx context.Context) (*cid.Set, error) {
	set := cid.NewSet()

	pinCh, errCh := c.pins.Ls(ctx)
	for pin := range pinCh {
		if pin.GetStatus() != httppin.StatusFailed {
			set.Add(pin.GetPin().GetCid())
		}
	}

	if err := <-errCh; err != nil {
		return nil, err
	}

	return set, nil
}

func (c *EstuaryClient) Export(ctx context.Context, id cid.Cid) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://dweb.link/api/v0/dag/export?arg="+id.String(), nil)
	if err != nil {
		return nil, err
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode > 299 {
		return nil, fmt.Errorf("failed to export status=%s", res.Status)
	}
	return res.Body, nil
}

func (c *EstuaryClient) Import(ctx context.Context, id cid.Cid, ir io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/content/add-car", ir)
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+c.key)

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode > 299 {
		return fmt.Errorf("failed to import to ipfs: %s", string(data))
	}
	return nil
}
