package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
)

type QueueInfo struct {
	Id             string
	State          string
	Error          string
	Project        int
	Run            int
	Clone          int
	Gen            int
	Core           string
	Unit           string
	Percentdone    string
	Eta            string
	Ppd            string
	Creditestimate string
	Waitingon      string
	Nextattempt    string
	Timeremaining  string
	Totalframes    int
	Framesdone     int
	Assigned       string
	Timeout        string
	Deadline       string
	Ws             string
	Cs             string
	Attempts       int
	Slot           string
	Tpf            string
	Basecredit     string
}

type SlotInfo struct {
	Id          string
	Status      string
	Description string
	// Options       ?? (not inspected yet)
	Reason string
	Idle   bool
}

type FAHWatcher struct {
	netConn net.Conn
	hbChan  chan int
	qiChan  chan QueueInfo
	siChan  chan SlotInfo
}

type SlotInfoStreamJSONifier struct {
	r                      io.Reader
	idleFieldMatchedLength int
}

func NewSlotInfoStreamJSONifier(r io.Reader) *SlotInfoStreamJSONifier {
	return &SlotInfoStreamJSONifier{r, 0}
}

// Since boolean literals are 'True' or 'False' in PyON format
// while they are 'true' or 'false' in JSON, conversion is necessary.
// Without conversion, `encoding/json` fails during scanner.go, which
// `UnmarshalJSON` is not affected.
func (s *SlotInfoStreamJSONifier) Read(p []byte) (int, error) {
	idleFieldMatcher := `"idle": `
	n, err := s.r.Read(p)
	if err != nil {
		return n, err
	}
	for i, c := range p[0:n] {
		if s.idleFieldMatchedLength < len(idleFieldMatcher) {
			if c == idleFieldMatcher[s.idleFieldMatchedLength] {
				s.idleFieldMatchedLength++
			} else {
				s.idleFieldMatchedLength = 0
			}
		} else {
			switch c {
			case 'T':
				p[i] = 't'
			case 'F':
				p[i] = 'f'
			}
			s.idleFieldMatchedLength = 0
		}
	}
	return n, nil
}

func Connect(endpoint string) (*FAHWatcher, chan error, error) {
	netConn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Connection used behind the communication with FAHClient raised error")
	}

	buf := make([]byte, 65536)
	_, err = netConn.Read(buf) // discard welcome message
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error occured during reading the first message from FAHClient")
	}

	hbChan := make(chan int)
	qiChan := make(chan QueueInfo)
	siChan := make(chan SlotInfo)
	errChan := make(chan error)
	go func() {
		for {
			err = rxMessageRouter(netConn, hbChan, qiChan, siChan)
			errChan <- err
			if err == io.EOF {
				return
			}
			time.Sleep(5 * time.Second)
		}
	}()
	return &FAHWatcher{netConn, hbChan, qiChan, siChan}, errChan, nil
}

type FAHUpdatesId int

const (
	heartbeatUpdatesId FAHUpdatesId = iota
	queueInfoUpdatesId
	slotInfoUpdatesId
)

func (fahw *FAHWatcher) WatchHeartbeat(interval int) (chan int, error) {
	command := fmt.Sprintf("updates add %d %d $heartbeat\n", heartbeatUpdatesId, interval)
	err := sendCommand(fahw.netConn, command)
	if err != nil {
		return nil, err
	}
	return fahw.hbChan, nil
}

func (fahw *FAHWatcher) WatchQueueInfo(interval int) (chan QueueInfo, error) {
	command := fmt.Sprintf("updates add %d %d $queue-info\n", queueInfoUpdatesId, interval)
	err := sendCommand(fahw.netConn, command)
	if err != nil {
		return nil, err
	}
	return fahw.qiChan, nil
}

func (fahw *FAHWatcher) WatchSlotInfo(interval int) (chan SlotInfo, error) {
	command := fmt.Sprintf("updates add %d %d $slot-info\n", slotInfoUpdatesId, interval)
	err := sendCommand(fahw.netConn, command)
	if err != nil {
		return nil, err
	}
	return fahw.siChan, nil
}

func sendCommand(netConn net.Conn, command string) error {
	_, err := netConn.Write([]byte(command))
	if err != nil {
		return errors.Wrapf(err, "Error occured during sending `%s` to FAHClient", command)
	}
	return nil
}

func rxMessageRouter(netConn net.Conn, hbChan chan int, qiChan chan QueueInfo, siChan chan SlotInfo) error {
	var reader io.Reader
	reader = netConn
	for {
		buf := make([]byte, 64)
		numRead, err := reader.Read(buf)
		if err == io.EOF {
			return err
		} else if err != nil {
			return errors.Wrap(err, "Cannot read next PyON data")
		}
		preambleBegin, err := findFirst(buf, 'P')
		if err != nil {
			// It might not have received PyON formatted result yet
			continue
		}
		preambleLength, err := findFirst(buf[preambleBegin:len(buf)], '\n')
		if err != nil {
			return errors.Wrap(err, "Error occured while finding the end of PyON preamble")
		}
		reader = io.MultiReader(bytes.NewReader(buf[preambleBegin+preambleLength+1:numRead]), reader)
		preamble := string(buf[preambleBegin : preambleBegin+preambleLength])
		var num int
		var pyon, msgType string
		fmt.Sscanf(string(preamble), "%s%d%s", &pyon, &num, &msgType)
		if pyon != "PyON" {
			return fmt.Errorf("PyON format error: buffer=%s", buf)
		}
		switch msgType {
		case "heartbeat":
			hbCnt, remainingReader, err := parseSimpleInteger(reader)
			if err != nil {
				return errors.Wrap(err, "Error occured while parsing heartbeat")
			}
			hbChan <- hbCnt
			reader = io.MultiReader(remainingReader, reader)
		case "units":
			qs, remainingReader, err := parseQueueInfo(reader)
			if err != nil {
				return errors.Wrap(err, "Error occured while parsing queue-info")
			}
			for _, q := range qs {
				qiChan <- q
			}
			reader = io.MultiReader(remainingReader, reader)
		case "slots":
			slots, remainingReader, err := parseSlotInfo(reader)
			if err != nil {
				return errors.Wrap(err, "Error occured while parsing slot-info")
			}
			for _, s := range slots {
				siChan <- s
			}
			reader = io.MultiReader(remainingReader, reader)
		}
		buf = make([]byte, 5)
		_, err = reader.Read(buf)
		if err != nil {
			return errors.Wrap(err, "Error occured while reading PyON footer")
		}
		if string(buf) != "\n---\n" {
			return fmt.Errorf("Unexpected array of bytes found while reading PyON footer")
		}
	}
}

func findFirst(array []byte, target byte) (int, error) {
	for i, c := range array {
		if c == target {
			return i, nil
		}
	}
	return 0, fmt.Errorf("Cannot find '%c'", target)
}

func parseSimpleInteger(r io.Reader) (int, io.Reader, error) {
	buf := make([]byte, 64)
	numRead, err := r.Read(buf)
	if err != nil {
		return 0, nil, errors.Wrap(err, "Failed to read")
	}
	newlinePos, err := findFirst(buf, '\n')
	if err != nil {
		return 0, nil, errors.Wrap(err, "Cannot find the end of data")
	}
	var intVal int
	_, err = fmt.Sscanf(string(buf[0:newlinePos]), "%d", &intVal)
	if err != nil {
		return 0, nil, errors.Wrap(err, "Failed to parse the data as integer value")
	}
	return intVal, bytes.NewReader(buf[newlinePos:numRead]), nil
}

func parseQueueInfo(r io.Reader) ([]QueueInfo, io.Reader, error) {
	dec := json.NewDecoder(r)
	var qs []QueueInfo
	err := dec.Decode(&qs)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error occured while queue-info result is parsed")
	}
	return qs, dec.Buffered(), nil
}

func parseSlotInfo(r io.Reader) ([]SlotInfo, io.Reader, error) {
	dec := json.NewDecoder(NewSlotInfoStreamJSONifier(r))
	var slots []SlotInfo
	err := dec.Decode(&slots)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error occured while slot-info result is parsed")
	}
	return slots, dec.Buffered(), nil
}
