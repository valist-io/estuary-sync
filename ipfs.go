package main

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	cid "github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
)

type IpfsClient struct {
	http *http.Client
	ipfs *httpapi.HttpApi
	url  string
}

func NewIpfsClient(url string) (*IpfsClient, error) {
	if url == "" {
		return nil, fmt.Errorf("ipfs url is required")
	}
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	ipfs, err := httpapi.NewURLApiWithClient(url, client)
	if err != nil {
		return nil, err
	}
	return &IpfsClient{
		http: client,
		ipfs: ipfs,
		url:  url,
	}, nil
}

func (c *IpfsClient) Pins(ctx context.Context) (*cid.Set, error) {
	pinCh, err := c.ipfs.Pin().Ls(ctx)
	if err != nil {
		return nil, err
	}

	set := cid.NewSet()
	for pin := range pinCh {
		if err := pin.Err(); err != nil {
			return nil, err
		}
		set.Add(pin.Path().Cid())
	}

	return set, nil
}

func (c *IpfsClient) Export(ctx context.Context, id cid.Cid) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/api/v0/dag/export?arg="+id.String(), nil)
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

func (c *IpfsClient) Import(ctx context.Context, id cid.Cid, ir io.Reader) error {
	// use a pipe to stream request data
	pr, pw := io.Pipe()
	// write form data to the pipe writer
	mw := multipart.NewWriter(pw)
	// pipe reader will block until content is written to pipe writer
	// this is how the data is able to be streamed in a routine
	go func() {
		defer mw.Close()
		// create a form file for the multipart data
		ff, err := mw.CreateFormFile("path", id.String())
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		// copy the input data to the form file
		_, err = io.Copy(ff, ir)
		if err != nil {
			pw.CloseWithError(err)	
			return
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/api/v0/dag/import", pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

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
