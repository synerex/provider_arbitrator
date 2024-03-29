package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"

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
	nodesrv                 = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local                   = flag.String("local", "", "Local Synerex Server")
	num                     = flag.Int("num", 1, "Number of Arbitrator")
	mu                      sync.Mutex
	version                 = "0.0.0"
	role                    = "Arbitrator"
	sxServerAddress         string
	TrafficAccident         = "TrafficAccident"
	rcmClient               *sxutil.SXServiceClient
	typeProp                = "type"
	departureTimeProp       = "departureTime"
	臨時便                     = "臨時便"
	ダイヤ調整                   = "ダイヤ調整"
	pendingSp               *api.Supply
	proposedSps             []*api.Supply
	supplySeleted           = false
	wantBusCanDiagramAdjust = false
	wantBusCanAddTemp       = false
	arbitratorStatus        = &ArbitratorStatus{}
	isWaitingBusSupply      = false
)

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

type ArbitratorStatus struct {
	ShouldSupplyTemp    bool   `json:"should_supply_temp"`
	ShouldSupplyAdjust  bool   `json:"should_supply_adjust"`
	BusStop             string `json:"busstop"`
	IsUp                bool   `json:"is_up"`
	IsStartingPoint     bool   `json:"is_starting_point"`
	TravelTime          int    `json:"travel_time"`
	Line                string `json:"line"`
	End                 string `json:"end"`
	ArrivalTime         int    `json:"arrival_time"`
	Next                string `json:"next"`
	DepartureTime       int    `json:"departure_time"`
	DemandDepartureTime int    `json:"demand_departure_time"`
	ID                  int    `json:"id"`
	Index               int    `json:"index"`
}

type BusCanDiagramAdjust struct {
	Want                bool   `json:"want"`
	Area                string `json:"area"`
	Index               int    `json:"index"`
	DemandDepartureTime int    `json:"demand_departure_time"`
}

type BusCanAdd struct {
	Want        bool   `json:"want"`
	FromStation string `json:"from_station"`
	ToStation   string `json:"to_station"`
}

func supplyRecommendDemandCallback(clt *sxutil.SXServiceClient, dm *api.Demand) {
	recommend := &rcm.Recommend{}
	if dm.Cdata != nil {
		err := proto.Unmarshal(dm.Cdata.Entity, recommend)
		if err == nil {
			log.Printf("Received Recommend Demand: Demand %+v, Recommend %+v", dm, recommend)
			if (*num == 1 && recommend.RecommendName == "A") || (*num == 2 && recommend.RecommendName == "B") {
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
		if ta.Type == gjson.String && ta.Str == 臨時便 && *num == 1 { // Arbitrator 1
			proposedSps = append(proposedSps, sp)
		}
		if ta.Type == gjson.String && ta.Str == ダイヤ調整 && *num == 2 { // Arbitrator 2
			pendingSp = sp
			log.Printf("Arbitrator %d: %s", *num, ta.Value())
			gess := &rcm.Recommend{
				RecommendId:   2,
				RecommendName: "B",
				RecommendSteps: []*rcm.RecommendStep{
					{
						MobilityType:  "bus",
						FromStationId: "b",
						ToStationId:   "e",
					},
				},
				DemandDepartureTime: uint32(arbitratorStatus.DemandDepartureTime),
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

func subscribeRecommendSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyRecommendSupplyCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func supplyJsonRecordCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	// log.Printf("Received JsonRecord Supply: Supply %+v, JSON: %s", sp, sp.ArgJson)
	ta := gjson.Get(sp.ArgJson, TrafficAccident)
	if ta.Type == gjson.JSON {
		log.Printf("Received JsonRecord Supply: Supply %+v, JSON: %s", sp, sp.ArgJson)

		if *num == 1 {
			if !isWaitingBusSupply {
				// if err == nil && err2 == nil && index > 0 {
				busstop := "B"
				next := "C"
				dmo := sxutil.DemandOpts{
					Name: role,
					JSON: fmt.Sprintf(`{ "type": "%s", "vehicle": "マイクロバス", "date": "ASAP", "from": "%s", "to": "%s", "stops": "none", "way": "round-trip", "repetition": 4 }`, 臨時便, busstop, next),
				}
				dmid, nerr := rcmClient.NotifyDemand(&dmo)
				if nerr != nil {
					log.Printf("#1 NotifyDemand Fail! %v\n", nerr)
				} else {
					log.Printf("#1 NotifyDemand OK! dmo: %#v, dmid: %d\n", dmo, dmid)
					arbitratorStatus = &ArbitratorStatus{
						ShouldSupplyTemp:   true,
						ShouldSupplyAdjust: false,
						BusStop:            busstop,
						Next:               next,
						DepartureTime:      9999,
					}
				}
				// }

				isWaitingBusSupply = true
				time.AfterFunc(5*time.Second, func() {
					isWaitingBusSupply = false
					// departureTime が最早の臨時便を選ぶ
					for _, sp := range proposedSps {
						departureTime := gjson.Get(sp.ArgJson, departureTimeProp)
						if departureTime.Type == gjson.Number && departureTime.Int() < int64(arbitratorStatus.DepartureTime) {
							pendingSp = sp
							arbitratorStatus.DepartureTime = int(departureTime.Int())
							arbitratorStatus.ArrivalTime = int(gjson.Get(sp.ArgJson, "arrivalTime").Int())
							arbitratorStatus.DemandDepartureTime = arbitratorStatus.DepartureTime
							arbitratorStatus.ID = int(gjson.Get(sp.ArgJson, "busID").Int())
							arbitratorStatus.IsUp = gjson.Get(sp.ArgJson, "isUp").Bool()
							arbitratorStatus.IsStartingPoint = gjson.Get(sp.ArgJson, "isStartingPoint").Bool()
							arbitratorStatus.TravelTime = int(gjson.Get(sp.ArgJson, "travelTime").Int())
							arbitratorStatus.Line = gjson.Get(sp.ArgJson, "line").String()
							arbitratorStatus.End = gjson.Get(sp.ArgJson, "end").String()
							arbitratorStatus.BusStop = gjson.Get(sp.ArgJson, "from").String()
						}
					}
					ta := gjson.Get(pendingSp.ArgJson, typeProp)
					gess := &rcm.Recommend{
						RecommendId:   1,
						RecommendName: "A",
						RecommendSteps: []*rcm.RecommendStep{
							{
								MobilityType:  "tempbus",
								FromStationId: "b",
								ToStationId:   "C",
							},
						},
						DemandDepartureTime: uint32(arbitratorStatus.DepartureTime),
					}
					log.Printf("Arbitrator %d: %s %v", *num, ta.Value(), gess)
					out, _ := proto.Marshal(gess)
					cont := api.Content{Entity: out}
					spo := sxutil.SupplyOpts{
						Name:  role,
						Cdata: &cont,
						JSON:  `{ "outlook": "2024/03/01 21:00", "cost": "600,000円", "CO2kg": 22.5 }`,
					}
					spid, nerr := rcmClient.NotifySupply(&spo)
					if nerr != nil {
						log.Printf("#3-1 NotifySupply Fail! %v\n", nerr)
					} else {
						log.Printf("#3-1 NotifySupply OK! spo: %#v, spid: %d\n", spo, spid)
					}
				})
			}
		}
		if *num == 2 {
			wantBusCanDiagramAdjust = true
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

// Arbitrator がダイヤ調整 or 臨時便調達をさせるかどうかをシミュレータに返す
func arbitratorStatusHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	step := r.URL.Query().Get("step")

	status := ArbitratorStatus{ShouldSupplyAdjust: false, ShouldSupplyTemp: false}
	if supplySeleted {
		status = *arbitratorStatus
		supplySeleted = false
	}

	log.Printf("Called /api/v0/arbitrator_status id: %s, step: %s -> Response: %+v\n", id, step, status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// シミュレータから Arbitrator にダイヤ調整できそうかどうかを返してもらう
func postBusCanDiagramAdjustHandler(w http.ResponseWriter, r *http.Request) {
	indexStr := r.URL.Query().Get("index")
	index, err := strconv.Atoi(indexStr)
	demandDepartureTimeStr := r.URL.Query().Get("demand_departure_time")
	demandDepartureTime, err2 := strconv.Atoi(demandDepartureTimeStr)

	if err == nil && err2 == nil && index > 0 {
		dmo := sxutil.DemandOpts{
			Name: role,
			JSON: fmt.Sprintf(`{ "index": %d , "demand_departure_time": %d, "type": "%s" }`, index, demandDepartureTime, ダイヤ調整),
		}
		dmid, nerr := rcmClient.NotifyDemand(&dmo)
		if nerr != nil {
			log.Printf("#1 NotifyDemand Fail! %v\n", nerr)
		} else {
			log.Printf("#1 NotifyDemand OK! dmo: %#v, dmid: %d\n", dmo, dmid)
			arbitratorStatus = &ArbitratorStatus{
				ShouldSupplyTemp:    false,
				ShouldSupplyAdjust:  true,
				DemandDepartureTime: demandDepartureTime,
				Index:               index,
			}
		}
	}

	status := BusCanDiagramAdjust{Want: false, Area: "B", Index: index, DemandDepartureTime: demandDepartureTime}

	log.Printf("Called /api/v0/post_bus_can_diagram_adjust -> Response: %+v\n", status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// Arbitrator がダイヤ調整を要求するかどうかをシミュレータに返す
func wantBusCanDiagramAdjustHandler(w http.ResponseWriter, r *http.Request) {
	status := BusCanDiagramAdjust{Want: false}
	if wantBusCanDiagramAdjust {
		status.Want = true
		status.Area = "B"
		wantBusCanDiagramAdjust = false
	}

	log.Printf("Called /api/v0/want_bus_can_diagram_adjust -> Response: %+v\n", status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// シミュレータから Arbitrator に臨時便を調達できそうかどうかを返してもらう
// 注：現在は臨時便の調達可否をシミュレータから受け取らないので、使ってない
func postBusCanAddTempHandler(w http.ResponseWriter, r *http.Request) {
	busstop := r.URL.Query().Get("busstop")
	isUp := r.URL.Query().Get("is_up") == "True"
	isStartingPoint := r.URL.Query().Get("is_starting_point") == "True"
	travelTimeStr := r.URL.Query().Get("travel_time")
	travelTime, err := strconv.Atoi(travelTimeStr)
	if err != nil {
		log.Printf("Error in travelTimeStr: %s, err: %v\n", travelTimeStr, err)
	}
	line := r.URL.Query().Get("line")
	end := r.URL.Query().Get("end")
	arrivalTimeStr := r.URL.Query().Get("arrival_time")
	arrivalTime, err := strconv.Atoi(arrivalTimeStr)
	if err != nil {
		log.Printf("Error in arrivalTimeStr: %s, err: %v\n", arrivalTimeStr, err)
	}
	next := r.URL.Query().Get("next")
	departureTimeStr := r.URL.Query().Get("departure_time")
	departureTime, err := strconv.Atoi(departureTimeStr)
	if err != nil {
		log.Printf("Error in departureTimeStr: %s, err: %v\n", departureTimeStr, err)
	}
	busIDStr := r.URL.Query().Get("bus_id")
	busID, err := strconv.Atoi(busIDStr)
	if err != nil {
		log.Printf("Error in busIDStr: %s, err: %v\n", busIDStr, err)
	}

	// if err == nil && err2 == nil && index > 0 {
	dmo := sxutil.DemandOpts{
		Name: role,
		JSON: fmt.Sprintf(`{ "type": "%s", "vehicle": "マイクロバス", "date": "ASAP", "from": "%s", "to": "%s", "stops": "none", "way": "round-trip", "repetition": 4 }`, 臨時便, busstop, next),
	}
	dmid, nerr := rcmClient.NotifyDemand(&dmo)
	if nerr != nil {
		log.Printf("#1 NotifyDemand Fail! %v\n", nerr)
	} else {
		log.Printf("#1 NotifyDemand OK! dmo: %#v, dmid: %d\n", dmo, dmid)
		arbitratorStatus = &ArbitratorStatus{
			ShouldSupplyTemp:   true,
			ShouldSupplyAdjust: false,
			BusStop:            busstop,
			IsUp:               isUp,
			IsStartingPoint:    isStartingPoint,
			TravelTime:         travelTime,
			Line:               line,
			End:                end,
			ArrivalTime:        arrivalTime,
			Next:               next,
			DepartureTime:      departureTime,
			ID:                 busID,
		}
	}
	// }

	status := BusCanAdd{Want: false, FromStation: busstop, ToStation: next}

	log.Printf("Called /api/v0/post_bus_can_add_temp -> Response: %+v\n", status)
	response, err := json.Marshal(status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

// Arbitrator が臨時便を要求するかどうかをシミュレータに返す
// 注：現在は臨時便の調達可否をシミュレータから受け取らないので、使ってない
func wantBusCanAddTempHandler(w http.ResponseWriter, r *http.Request) {
	status := BusCanAdd{Want: false}
	if wantBusCanAddTemp {
		status.Want = true
		status.FromStation = "B"
		status.ToStation = "C"
		wantBusCanAddTemp = false
	}

	log.Printf("Called /api/v0/want_bus_can_add_temp -> Response: %+v\n", status)
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
	http.HandleFunc("/api/v0/want_bus_can_diagram_adjust", wantBusCanDiagramAdjustHandler)
	http.HandleFunc("/api/v0/post_bus_can_diagram_adjust", postBusCanDiagramAdjustHandler)
	http.HandleFunc("/api/v0/want_bus_can_add_temp", wantBusCanAddTempHandler)
	http.HandleFunc("/api/v0/post_bus_can_add_temp", postBusCanAddTempHandler)
	fmt.Printf("Server is running on port 804%d\n", *num)
	go http.ListenAndServe(fmt.Sprintf(":804%d", *num), nil)
	wg.Wait()
}
