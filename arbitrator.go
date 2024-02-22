package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	rcm "github.com/synerex/proto_recommend"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"log"
	"sync"
	"time"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	num             = flag.Int("num", 1, "Number of Arbitrator")
	mu              sync.Mutex
	version         = "0.0.0"
	role            = "Arbitrator"
	sxServerAddress string
	TrafficAccident = "TrafficAccident"
	rcmClient       *sxutil.SXServiceClient
	typeProp        = "type"
	臨時便             = "臨時便"
	pendingSp       *api.Supply
	supplySeleted   = false
)

func init() {
	flag.Parse()
}

type ArbitratorStatus struct {
	ShouldSupply    bool   `json:"should_supply"`
	BusStop         string `json:"busstop"`
	IsUp            bool   `json:"is_up"`
	IsStartingPoint bool   `json:"is_starting_point"`
	TravelTime      int    `json:"travel_time"`
	Line            string `json:"line"`
	End             string `json:"end"`
	ArrivalTime     int    `json:"arrival_time"`
	Next            string `json:"next"`
	DepartureTime   int    `json:"departure_time"`
	ID              int    `json:"id"`
}

func supplyRecommendDemandCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	recommend := &rcm.Recommend{}
	if dm.Cdata != nil {
		err := proto.Unmarshal(dm.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Demand: Demand %+v, Recommend %+v", dm, recommend)
			if *num == 1 && recommend.RecommendName == "A" {
				dmid, nerr := clt.SelectDemand(dm)
				if nerr != nil {
					log.Printf("#5 SelectDemand Fail! %v\n", nerr)
				} else {
					log.Printf("#5 SelectDemand OK! dm: %#v, dmid: %d\n", dm, dmid)
					spid, nerr := clt.SelectSupply(pendingSp)
					if nerr != nil {
						log.Printf("#7 SelectSupply Fail! %v\n", nerr)
					} else {
						log.Printf("#7 SelectSupply OK! sp: %#v, spid: %d\n", pendingSp, spid)
						supplySeleted = true
					}
				}
			}
		}
	} else {
		log.Printf("Received JsonRecord Demand: Demand %+v, JSON: %s", dm, dm.ArgJson)
	}
}

func supplyRecommendSupplyCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	recommend := &rcm.Recommend{}
	if sp.Cdata != nil {
		err := proto.Unmarshal(sp.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Supply: Supply %+v, Recommend %+v", sp, recommend)
		}
	} else {
		log.Printf("Received JsonRecord Supply: Supply %+v, JSON: %s", sp, sp.ArgJson)
		ta := gjson.Get(sp.ArgJson, typeProp)
		if ta.Type == gjson.String && ta.Str == 臨時便 {
			pendingSp = sp
			log.Printf("臨時便: %+v", ta.Value())
			if *num == 1 {
				gess := &rcm.Recommend{
					RecommendId:   1,
					RecommendName: "A",
					RecommendSteps: []*rcm.RecommendStep{
						{
							MobilityType:  1,
							FromStationId: 2,
							ToStationId:   3,
						},
						{
							MobilityType:  4,
							FromStationId: 5,
							ToStationId:   6,
						},
					},
				}
				out, _ := proto.Marshal(gess)
				cont := api.Content{Entity: out}
				spo := sxutil.SupplyOpts{
					Name:  role,
					Cdata: &cont,
					JSON:  `{ "outlook": "2024/03/01 21:00", "cost": "600,000円", "CO2kg": 22.5 }`,
				}
				spid, nerr := clt.NotifySupply(&spo)
				if nerr != nil {
					log.Printf("#3-1 NotifySupply Fail! %v\n", nerr)
				} else {
					log.Printf("#3-1 NotifySupply OK! spo: %#v, spid: %d\n", spo, spid)
				}
			}
			if *num == 2 {
				gess := &rcm.Recommend{
					RecommendId:   2,
					RecommendName: "B",
					RecommendSteps: []*rcm.RecommendStep{
						{
							MobilityType:  7,
							FromStationId: 8,
							ToStationId:   9,
						},
						{
							MobilityType:  10,
							FromStationId: 11,
							ToStationId:   12,
						},
					},
				}
				out, _ := proto.Marshal(gess)
				cont := api.Content{Entity: out}
				spo := sxutil.SupplyOpts{
					Name:  role,
					Cdata: &cont,
					JSON:  `{ "outlook": "2024/03/01 23:00", "cost": "900,000円", "CO2kg": 2.0 }`,
				}
				spid, nerr := clt.NotifySupply(&spo)
				if nerr != nil {
					log.Printf("#3-2 NotifySupply Fail! %v\n", nerr)
				} else {
					log.Printf("#3-2 NotifySupply OK! spo: %#v, spid: %d\n", spo, spid)
				}
			}
		}
	}
}

func subscribeRecommendSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyRecommendSupplyCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func supplyJsonRecordCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	log.Printf("Received JsonRecord Supply: Supply %+v, JSON: %s", sp, sp.ArgJson)
	ta := gjson.Get(sp.ArgJson, TrafficAccident)
	if ta.Type == gjson.JSON {
		log.Printf("TrafficAccident: %+v", ta.Value())

		if *num == 1 {

			url := fmt.Sprintf(`http://host.docker.internal:5000/api/v0/bus_can_diagram_adjust`)
			resp, err := http.Get(url)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			var result map[string]interface{}
			err = json.Unmarshal([]byte(body), &result)
			if err != nil {
				fmt.Println("Error parsing JSON:", err)
				return
			}

			index, ok := result["index"].(int)
			if !ok {
				fmt.Println("Error: index is not a number, defaulting to 0")
				index = 0
			} else {
				fmt.Printf("ID: %d\n", int(index))
			}

			demandDepartureTime, ok := result["demand_departure_time"].(int)
			if !ok {
				fmt.Println("Error: demand_departure_time is not a number, defaulting to 0")
				demandDepartureTime = 0
			} else {
				fmt.Printf("Demand Departure Time: %d\n", int(demandDepartureTime))
			}

			if index > 0 {
				dmo := sxutil.DemandOpts{
					Name: role,
					JSON: fmt.Sprintf(`{ "index": %d , "demand_departure_time": %d, "type": "臨時便", "vehicle": "マイクロバス", "date": "ASAP", "from": "岩倉駅", "to": "江南駅", "stops": "none", "way": "round-trip", "repetition": 4 }`, index, demandDepartureTime),
				}
				dmid, nerr := rcmClient.NotifyDemand(&dmo)
				if nerr != nil {
					log.Printf("#1 NotifyDemand Fail! %v\n", nerr)
				} else {
					log.Printf("#1 NotifyDemand OK! dmo: %#v, dmid: %d\n", dmo, dmid)
				}
			}
		}
	}
}

func subscribeJsonRecordSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyJsonRecordCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.SXClient != nil {
		client.SXClient = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.SXClient == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.SXClient = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server [%s]\n", sxServerAddress)
	}
	mu.Unlock()
}

func arbitratorStatusHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	step := r.URL.Query().Get("step")
	log.Printf("Called /api/v0/arbitrator_status id: %s, step: %s\n", id, step)

	status := ArbitratorStatus{ShouldSupply: false}
	if supplySeleted {
		status.ShouldSupply = true
		status.BusStop = "b"
		status.IsUp = false
		status.IsStartingPoint = true
		status.TravelTime = 14
		status.Line = "bus"
		status.End = "e"
		status.ArrivalTime = 1
		status.Next = "e"
		status.DepartureTime = 3
		status.ID = 0
		supplySeleted = false
	}
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func main() {
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("%s%d(%s) built %s sha1 %s", role, *num, sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.ALT_PT_SVC, pbase.JSON_DATA_SVC}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, fmt.Sprintf("%s%d", role, *num), channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	} else {
		log.Print("Connecting SynerexServer")
	}

	rcmClient = sxutil.NewSXServiceClient(client, pbase.ALT_PT_SVC, fmt.Sprintf("{Client:%s%d}", role, *num))
	envClient := sxutil.NewSXServiceClient(client, pbase.JSON_DATA_SVC, fmt.Sprintf("{Client:%s%d}", role, *num))

	wg.Add(1)
	log.Print("Subscribe Supply")
	go subscribeRecommendSupply(rcmClient)
	sxutil.SimpleSubscribeDemand(rcmClient, supplyRecommendDemandCallback)
	go subscribeJsonRecordSupply(envClient)

	http.HandleFunc("/api/v0/arbitrator_status", arbitratorStatusHandler)
	fmt.Println("Server is running on port 804%d", *num)
	go http.ListenAndServe(fmt.Sprintf(":804%d", *num), nil)
	wg.Wait()
}
