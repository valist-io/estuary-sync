package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	estuaryKey := os.Getenv("ESTUARY_API_KEY")
	if estuaryKey == "" {
		panic("Missing ESTUARY_API_KEY")
	}

	req, _ := http.NewRequest("GET", "https://api.estuary.tech/pinning/pins", nil)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+estuaryKey)
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println("Errored when sending request to estuary")
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

	req, _ := http.NewRequest("POST", ipfsHost+"/api/v0/pin/ls", nil)
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println("Errored when sending request to IPFS Node")
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

func addEstuaryPin(client *http.Client, pin Pin) (string, string) {
	estuaryKey := os.Getenv("ESTUARY_API_KEY")
	if estuaryKey == "" {
		panic("Missing ESTUARY_API_KEY")
	}

	reqBody, err := json.Marshal(pin)
	if err != nil {
		panic(err)
	}
	reqReader := bytes.NewReader(reqBody)

	req, _ := http.NewRequest("POST", "https://api.estuary.tech/pinning/pins", reqReader)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer "+estuaryKey)
	resp, err := client.Do(req)

	if err != nil {
		fmt.Println("Errored when sending request to estuary")
		panic(err)
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	return resp.Status, string(body)
}

func main() {
	client := &http.Client{}

	estuaryResults := fetchEstuaryPins(client)
	ipfsNodeResults := fetchIPFSNodePins(client)

	pinSet := make(PinSet)

	// var estuaryCids []string

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

	// var ipfsNodeCids []string

	// for cid := range ipfsNodeResults.Keys {
	// 	ipfsNodeCids = append(ipfsNodeCids, cid)
	// }

	for i, pin := range inIPFSNode {
		status, res := addEstuaryPin(client, pin)
		fmt.Println(status, res)
		if i == 5 {
			break
		}
	}

	// fmt.Println(ipfsNodeCids)

}
