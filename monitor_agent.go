package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
)

type CliArgs struct {
	ClientName    string
	FAHClientPort string
	EsURLs        string
}

func parseCliArgs() CliArgs {
	var clientName, clientPort, esURLs string
	flag.StringVar(&clientName, "client-name", "", "Name of Folding@home Client for identification")
	flag.StringVar(&clientPort, "client-port", "localhost:36330", "Name of Folding@home Client for identification")
	flag.StringVar(&esURLs, "elasticsearch", "http://localhost:9200/", "comma-separated URL list of elasticsearch")
	flag.Parse()
	return CliArgs{clientName, clientPort, esURLs}
}

type HeartbeatForEs struct {
	Timestamp  string `json:"@timestamp"`
	ClientName string `json:"client_name"`
	Counter    int    `json:"counter"`
}

func formatHeartbeatForEs(heartbeat int, clientName string) HeartbeatForEs {
	return HeartbeatForEs{
		Timestamp:  time.Now().Format(time.RFC3339),
		ClientName: clientName,
		Counter:    heartbeat,
	}
}

type QueueInfoForEs struct {
	Timestamp      string  `json:"@timestamp"`
	ClientName     string  `json:"client_name"`
	ClientSlot     string  `json:"client_slot"`
	Project        int     `json:"project"`
	Run            int     `json:"run"`
	Clone          int     `json:"clone"`
	Gen            int     `json:"gen"`
	PRCG           string  `json:"prcg"`
	State          string  `json:"state"`
	Error          string  `json:"error"`
	Core           string  `json:"core"`
	Unit           string  `json:"unit"`
	PercentDone    float32 `json:"percent_done"`
	ETA            string  `json:"eta"`
	PPD            int     `json:"point_per_day"`
	CreditEstimate int     `json:"credit_estimate"`
	WaitingOn      string  `json:"wating_on"`
	NextAttempt    string  `json:"next_attempt"`
	TimeRemaining  string  `json:"time_remaining"`
	TotalFrames    int     `json:"total_frames"`
	FramesDone     int     `json:"frames_done"`
	Assigned       string  `json:"assigned"`
	Timeout        string  `json:"timeout"`
	Deadline       string  `json:"deadline"`
	Ws             string  `json:"ws"`
	Cs             string  `json:"cs"`
	Attempts       int     `json:"attempts"`
	Slot           string  `json:"slot"`
	Tpf            string  `json:"tpf"`
	BaseCredit     int     `json:"base_credit"`
}

type SlotInfoForEs struct {
	Timestamp   string `json:"@timestamp"`
	ClientName  string `json:"client_name"`
	ClientSlot  string `json:"client_slot"`
	Id          string `json:"id"`
	Status      string `json:"status"`
	Description string `json:"description"`
	Reason      string `json:"reason"`
	Idle        bool   `json:"idle"`
}

func esIndexSuffix() string {
	t := time.Now()
	return fmt.Sprintf("_%d-%d", t.Year(), t.Month())
}

func formatQueueInfoForEs(qi QueueInfo, clientName string) QueueInfoForEs {

	var percentDone float32
	_, err := fmt.Sscanf(qi.Percentdone, "%f%%", &percentDone)
	if err != nil {
	}
	ppd, err := strconv.Atoi(qi.Ppd)
	creditEstimate, err := strconv.Atoi(qi.Creditestimate)
	if err != nil {
	}
	baseCredit, err := strconv.Atoi(qi.Basecredit)
	if err != nil {
	}
	return QueueInfoForEs{
		Timestamp:      time.Now().Format(time.RFC3339),
		ClientName:     clientName,
		ClientSlot:     clientName + "/" + qi.Slot,
		Project:        qi.Project,
		Run:            qi.Run,
		Clone:          qi.Clone,
		Gen:            qi.Gen,
		PRCG:           fmt.Sprintf("(%d,%d,%d,%d)", qi.Project, qi.Run, qi.Clone, qi.Gen),
		State:          qi.State,
		Error:          qi.Error,
		Core:           qi.Core,
		Unit:           qi.Unit,
		PercentDone:    percentDone,
		ETA:            qi.Eta,
		PPD:            ppd,
		CreditEstimate: creditEstimate,
		WaitingOn:      qi.Waitingon,
		NextAttempt:    qi.Nextattempt,
		TimeRemaining:  qi.Timeremaining,
		TotalFrames:    qi.Totalframes,
		FramesDone:     qi.Framesdone,
		Assigned:       qi.Assigned,
		Timeout:        qi.Timeout,
		Deadline:       qi.Deadline,
		Ws:             qi.Ws,
		Cs:             qi.Cs,
		Attempts:       qi.Attempts,
		Slot:           qi.Slot,
		Tpf:            qi.Tpf,
		BaseCredit:     baseCredit,
	}
}

func formatSlotInfoForEs(si SlotInfo, clientName string) SlotInfoForEs {
	return SlotInfoForEs{
		Timestamp:   time.Now().Format(time.RFC3339),
		ClientName:  clientName,
		ClientSlot:  clientName + "/" + si.Id,
		Id:          si.Id,
		Status:      si.Status,
		Description: si.Description,
		Reason:      si.Reason,
		Idle:        si.Idle,
	}
}

func main() {
	cliArgs := parseCliArgs()
	esCfg := elasticsearch.Config{
		Addresses: strings.Split(cliArgs.EsURLs, ","),
	}
	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		log.Fatal(err)
	}

	fahw, errChan, err := Connect(cliArgs.FAHClientPort)
	if err != nil {
		log.Fatal(err)
	}

	hbChan, err := fahw.WatchHeartbeat(60)
	qiChan, err := fahw.WatchQueueInfo(30)
	siChan, err := fahw.WatchSlotInfo(30)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case hb := <-hbChan:
			body, err := json.Marshal(formatHeartbeatForEs(hb, cliArgs.ClientName))
			if err != nil {
				log.Print(err)
				continue
			}
			go func() {
				res, err := es.Index("foldingathome-heartbeat"+esIndexSuffix(), bytes.NewReader(body))
				if err != nil {
					log.Print(err)
				}
				res.Body.Close()
			}()
		case qi := <-qiChan:
			body, err := json.Marshal(formatQueueInfoForEs(qi, cliArgs.ClientName))
			if err != nil {
				log.Print(err)
				continue
			}
			go func() {
				res, err := es.Index("foldingathome-queueinfo"+esIndexSuffix(), bytes.NewReader(body))
				if err != nil {
					log.Print(err)
				}
				res.Body.Close()
			}()
		case si := <-siChan:
			body, err := json.Marshal(formatSlotInfoForEs(si, cliArgs.ClientName))
			if err != nil {
				log.Print(err)
				continue
			}
			go func() {
				res, err := es.Index("foldingathome-slotinfo"+esIndexSuffix(), bytes.NewReader(body))
				if err != nil {
					log.Print(err)
				}
				res.Body.Close()
			}()
		case err := <-errChan:
			if err == io.EOF {
				log.Fatal(err)
			}
			log.Print(err)
		}
	}
}
