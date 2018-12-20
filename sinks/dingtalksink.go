package sinks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/eapache/channels"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/sethgrid/pester"
	"k8s.io/api/core/v1"
)

const (
	WARNING           int = 2
	NORMAL            int = 1
	DEFAULT_MSG_TYPE      = "text"
	CONTENT_TYPE_JSON     = "application/json"
	MSG_TEMPLATE          = "Level:%s \nCluster:%s \nNamespace:%s \nName:%s \nMessage:%s \nReason:%s \nTimestamp:%s"
)

type DingTalkNotificationResponse struct {
	ErrorMessage string `json:"errmsg"`
	ErrorCode    int    `json:"errcode"`
}

type DingTalkNotification struct {
	MessageType string                          `json:"msgtype"`
	Text        *DingTalkNotificationText       `json:"text,omitempty"`
	Link        *DingTalkNotificationLink       `json:"link,omitempty"`
	Markdown    *DingTalkNotificationMarkdown   `json:"markdown,omitempty"`
	ActionCard  *DingTalkNotificationActionCard `json:"actionCard,omitempty"`
	At          *DingTalkNotificationAt         `json:"at,omitempty"`
}

type DingTalkNotificationText struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type DingTalkNotificationLink struct {
	Title      string `json:"title"`
	Text       string `json:"text"`
	MessageURL string `json:"messageUrl"`
	PictureURL string `json:"picUrl"`
}

type DingTalkNotificationMarkdown struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

type DingTalkNotificationAt struct {
	AtMobiles []string `json:"atMobiles,omitempty"`
	IsAtAll   bool     `json:"isAtAll,omitempty"`
}

type DingTalkNotificationActionCard struct {
	Title             string                       `json:"title"`
	Text              string                       `json:"text"`
	HideAvatar        string                       `json:"hideAvatar"`
	ButtonOrientation string                       `json:"btnOrientation"`
	Buttons           []DingTalkNotificationButton `json:"btns,omitempty"`
	SingleTitle       string                       `json:"singleTitle,omitempty"`
	SingleURL         string                       `json:"singleURL"`
}

type DingTalkNotificationButton struct {
	Title     string `json:"title"`
	ActionURL string `json:"actionURL"`
}

// DingtalkSink wraps an dingtalk webhook endpoint that messages should be sent to.
type DingtalkSink struct {
	SinkURL string
	Cluster string
	Level int

	eventCh    channels.Channel
	httpClient *pester.Client
}

// NewDingtalkSink constructs a new DingtalkSink given a sink URL and buffer size
func NewDingtalkSink(sinkURL, cluster , level string, overflow bool, bufferSize int) *DingtalkSink {
	h := &DingtalkSink{
		SinkURL: sinkURL,
		Cluster: cluster,
	}

	if overflow {
		h.eventCh = channels.NewOverflowingChannel(channels.BufferCap(bufferSize))
	} else {
		h.eventCh = channels.NewNativeChannel(channels.BufferCap(bufferSize))
	}

	h.httpClient = pester.New()
	h.httpClient.Backoff = pester.ExponentialJitterBackoff
	h.httpClient.MaxRetries = 10

	return h
}

// UpdateEvents implements the EventSinkInterface. It really just writes the
// event data to the event OverflowingChannel, which should never block.
// Messages that are buffered beyond the bufferSize specified for this HTTPSink
// are discarded.
func (h *DingtalkSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {
	h.eventCh.In() <- NewEventData(eNew, eOld)
}

// Run sits in a loop, waiting for data to come in through h.eventCh,
// and forwarding them to the HTTP sink. If multiple events have happened
// between loop iterations, it puts all of them in one request instead of
// making a single request per event.
func (h *DingtalkSink) Run(stopCh <-chan bool) {
loop:
	for {
		select {
		case e := <-h.eventCh.Out():
			var evt EventData
			var ok bool
			if evt, ok = e.(EventData); !ok {
				glog.Warningf("Invalid type sent through event channel: %T", e)
				continue loop
			}

			// Start with just this event...
			arr := []EventData{evt}

			// Consume all buffered events into an array, in case more have been written
			// since we last forwarded them
			numEvents := h.eventCh.Len()
			for i := 0; i < numEvents; i++ {
				e := <-h.eventCh.Out()
				if evt, ok = e.(EventData); ok {
					arr = append(arr, evt)
				} else {
					glog.Warningf("Invalid type sent through event channel: %T", e)
				}
			}

			h.drainEvents(arr)
		case <-stopCh:
			break loop
		}
	}
}

// drainEvents takes an array of event data and sends it to the receiving HTTP
// server. This function is *NOT* re-entrant: it re-uses the same body buffer
// for each call, truncating it each time to avoid extra memory allocations.
func (h *DingtalkSink) drainEvents(events []EventData) {
	for _, event := range events {
		if !h.isEventLevelDangerous(event.Event.Type) {
			continue
		}

		notification, err := h.buildDingTalkNotification(event.Event)
		if err != nil {
			glog.Warningf("Failed to build notification : %T", err)
			continue
		}

		robotResp, err := h.sendDingTalkNotification(h.httpClient, h.SinkURL, notification)
		if err != nil {
			glog.Warningf("Failed to send notification: %T", err)
			continue
		}

		if robotResp.ErrorCode != 0 {
			glog.Warningf("Failed to send notification to DingTalk: respCode is %s and respMsg is %s", robotResp.ErrorCode, robotResp.ErrorMessage)
			continue
		}

	}

}

func (d *DingtalkSink) isEventLevelDangerous(level string) bool {
	score := getLevel(level)
	if score >= d.Level {
		return true
	}
	return false
}

func (d *DingtalkSink) buildDingTalkNotification(event *v1.Event) (*DingTalkNotification, error) {
	notification := &DingTalkNotification{
		MessageType: DEFAULT_MSG_TYPE,
		Text: &DingTalkNotificationText{
			Content: fmt.Sprintf(MSG_TEMPLATE, event.Type, d.Cluster,event.Namespace, event.Name, event.Message, event.Reason, event.LastTimestamp),
		},
	}
	return notification, nil
}

func (d *DingtalkSink) sendDingTalkNotification(httpClient *pester.Client, webhookURL string, notification *DingTalkNotification) (*DingTalkNotificationResponse, error) {
	body, err := json.Marshal(&notification)
	if err != nil {
		return nil, errors.Wrap(err, "error encoding DingTalk request")
	}

	httpReq, err := http.NewRequest("POST", webhookURL, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "error building DingTalk request")
	}
	httpReq.Header.Set("Content-Type", CONTENT_TYPE_JSON)

	req, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "error sending notification to DingTalk")
	}
	defer req.Body.Close()

	if req.StatusCode != 200 {
		return nil, errors.Errorf("unacceptable response code %d", req.StatusCode)
	}

	var robotResp DingTalkNotificationResponse
	enc := json.NewDecoder(req.Body)
	if err := enc.Decode(&robotResp); err != nil {
		return nil, errors.Wrap(err, "error decoding response from DingTalk")
	}

	return &robotResp, nil
}


func getLevel(level string) int {
	score := 0
	switch level {
	case v1.EventTypeWarning:
		score += 2
	case v1.EventTypeNormal:
		score += 1
	default:
		//score will remain 0
	}
	return score
}