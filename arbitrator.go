package main

import (
	"context"
	"flag"
	"fmt"

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
)

func init() {
	flag.Parse()
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
			dmo := sxutil.DemandOpts{
				Name: role,
				JSON: `{ "type": "臨時便", "vehicle": "マイクロバス", "date": "ASAP", "from": "岩倉駅", "to": "江南駅", "stops": "none", "way": "round-trip", "repetition": 4 }`,
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
	wg.Wait()
}
