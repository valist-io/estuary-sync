package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

type EstuaryResults struct {
	Count   int `json:"count"`
	Results []struct {
		Requestid string    `json:"requestid"`
		Status    string    `json:"status"`
		Created   time.Time `json:"created"`
		Pin       struct {
			Cid     string      `json:"cid"`
			Name    string      `json:"name"`
			Origins interface{} `json:"origins"`
			Meta    interface{} `json:"meta"`
		} `json:"pin"`
		Delegates []string    `json:"delegates"`
		Info      interface{} `json:"info"`
	} `json:"results"`
}

type IPFSNodeResults struct {
	Keys map[string]map[string]string
}

type PinSet map[string]struct {
	Estuary  bool
	IPFSNode bool
}

type Pin struct {
	Name string `json:"name"`
	Cid  string `json:"cid"`
}

func fetchEstuaryPins(client *http.Client) EstuaryResults {
	ipfsHost := os.Getenv("IPFS_HOST")
	if ipfsHost == "" {
		panic("Missing IPFS_HOST")
	}

	estuaryKey := os.Getenv("ESTUARY_API_KEY")
	if estuaryKey == "" {
		panic("Missing ESTUARY_API_KEY")
	}

	req, _ := http.NewRequest(http.MethodGet, "https://api.estuary.tech/pinning/pins", nil)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+estuaryKey)
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println("Errored when sending request to Estuary")
		panic(err)
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	var results EstuaryResults
	if err := json.Unmarshal(body, &results); err != nil {
		panic(err)
	}
	return results
}

func fetchIPFSNodePins(client *http.Client) IPFSNodeResults {
	ipfsHost := os.Getenv("IPFS_HOST")
	if ipfsHost == "" {
		panic("Missing IPFS_HOST")
	}

	estuaryKey := os.Getenv("ESTUARY_API_KEY")
	if estuaryKey == "" {
		panic("Missing ESTUARY_API_KEY")
	}

	req, _ := http.NewRequest(http.MethodPost, ipfsHost+"/api/v0/pin/ls", nil)
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println("Errored when fetching pins from IPFS Node")
		panic(err)
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	var results IPFSNodeResults
	if err := json.Unmarshal(body, &results); err != nil {
		panic(err)
	}
	return results
}

func streamCarToEstuary(client *http.Client, pin Pin) (string, string, error) {
	ipfsHost := os.Getenv("IPFS_HOST")
	if ipfsHost == "" {
		panic("Missing IPFS_HOST")
	}

	estuaryKey := os.Getenv("ESTUARY_API_KEY")
	if estuaryKey == "" {
		panic("Missing ESTUARY_API_KEY")
	}

	ipfsReq, err := http.NewRequest(http.MethodPost, ipfsHost+"/api/v0/dag/export?arg="+pin.Cid, nil)
	if err != nil {
		fmt.Println("Error when creating request to IPFS Node")
		return "", "", err
	}

	ipfsReq.Header.Add("Content-Type", "application/json")
	ipfsResp, err := client.Do(ipfsReq)
	if err != nil {
		fmt.Println("Error when sending request to IPFS Node")
		return "", "", err
	}

	defer ipfsResp.Body.Close()

	fmt.Println(fmt.Sprintf("Pinning %v to Estuary", pin.Cid))

	estReq, err := http.NewRequest(http.MethodPost, "https://api.estuary.tech/content/add-car", ipfsResp.Body)
	if err != nil {
		fmt.Println("Error when creating request to Estuary")
		return "", "", err
	}
	estReq.Header.Add("Content-Type", "application/json")
	estReq.Header.Add("Authorization", "Bearer "+estuaryKey)
	estResp, err := client.Do(estReq)
	if err != nil {
		fmt.Println("Error when sending request to Estuary")
		return "", "", err
	}

	defer estResp.Body.Close()
	body, err := ioutil.ReadAll(estResp.Body)
	if err != nil {
		fmt.Println("Error when reading Estuary response")
		return "", "", err
	}

	return estResp.Status, string(body), nil
}

func streamEstuaryToIPFSNode(client *http.Client, pin Pin) (string, string, error) {
	ipfsHost := os.Getenv("IPFS_HOST")
	if ipfsHost == "" {
		panic("Missing IPFS_HOST")
	}

	fmt.Println(fmt.Sprintf("Requesting %v CAR from Estuary", pin.Cid))

	estReq, err := http.NewRequest(http.MethodGet, "https://dweb.link/api/v0/dag/export?arg="+pin.Cid, nil)
	if err != nil {
		fmt.Println("Error when creating request to Estuary gateway")
		return "", "", err
	}

	estReq.Header.Add("Content-Type", "application/json")
	estResp, err := client.Do(estReq)
	if err != nil {
		fmt.Println("Error when sending request to Estuary gateway")
		return "", "", err
	}

	defer estResp.Body.Close()

	fmt.Println(fmt.Sprintf("Pinning %v to IPFS Node", pin.Cid))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("path", pin.Cid)
	if err != nil {
		fmt.Println("Error when creating form file")
		return "", "", err
	}

	_, err = io.Copy(part, estResp.Body)
	if err != nil {
		fmt.Println("Error when copying file parts")
		return "", "", err
	}

	err = writer.Close()
	if err != nil {
		fmt.Println("Error when closing writer")
		return "", "", err
	}

	ipfsReq, err := http.NewRequest(http.MethodPost, ipfsHost+"/api/v0/dag/import", body)
	if err != nil {
		fmt.Println("Error when creating request to IPFS Node")
		return "", "", err
	}
	ipfsReq.Header.Set("Content-Type", writer.FormDataContentType())

	ipfsResp, err := client.Do(ipfsReq)
	if err != nil {
		fmt.Println("Error when sending request to IPFS Node")
		return "", "", err
	}
	defer ipfsResp.Body.Close()

	ipfsBody, err := ioutil.ReadAll(ipfsResp.Body)
	if err != nil {
		fmt.Println("Error when reading IPFS Node response")
		return "", "", err
	}

	return ipfsResp.Status, string(ipfsBody), nil
}

func main() {

	client := &http.Client{}

	fmt.Println("Fetching Pin set from Estuary...")
	estuaryResults := fetchEstuaryPins(client)

	fmt.Println("Fetching Pin set from IPFS Node...")
	ipfsNodeResults := fetchIPFSNodePins(client)

	pinSet := make(PinSet)

	for _, result := range estuaryResults.Results {
		if result.Status == "pinned" {
			entry := pinSet[result.Pin.Cid]
			entry.Estuary = true
			pinSet[result.Pin.Cid] = entry
		}
	}

	for cid := range ipfsNodeResults.Keys {
		entry := pinSet[cid]
		entry.IPFSNode = true
		pinSet[cid] = entry
	}

	var inEstuary []Pin
	var inIPFSNode []Pin
	var inBoth []Pin

	for cid, locations := range pinSet {
		if locations.Estuary && locations.IPFSNode {
			inBoth = append(inBoth, Pin{Name: cid, Cid: cid})
		}
		if locations.Estuary && !locations.IPFSNode {
			inEstuary = append(inEstuary, Pin{Name: cid, Cid: cid})
		}
		if !locations.Estuary && locations.IPFSNode {
			inIPFSNode = append(inIPFSNode, Pin{Name: cid, Cid: cid})
		}
	}

	fmt.Println("Syncing from Estuary to IPFS Node...")

	for _, pin := range inEstuary {
		status, res, err := streamEstuaryToIPFSNode(client, pin)
		if err != nil {
			fmt.Println(fmt.Sprintf("Warning: Failed to pin %v %v", pin.Cid, err))
		}
		fmt.Println(status, res)
	}

	fmt.Println("Syncing IPFS Node to Estuary...")

	for _, pin := range inIPFSNode {
		status, res, err := streamCarToEstuary(client, pin)
		if err != nil {
			fmt.Println(fmt.Sprintf("Warning: Failed to pin %v %v", pin.Cid, err))
		}
		fmt.Println(status, res)
	}
}
