package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

type LokiPushRequest struct {
	Streams []LokiStream `json:"streams"`
}

type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

const (
	scrollBatchSize = 1000
)

var (
	scrollDuration time.Duration
)

func init() {
	var err error
	scrollDuration, err = time.ParseDuration("1m")
	if err != nil {
		log.Fatalf("Error parsing scroll duration: %v", err)
	}
}

func main() {
	var openSearchURL string
	var lokiPushURL string
	var openSearchIndex string
	for _, arg := range os.Args[1:] {
		if matched, _ := regexp.MatchString(`^--output=(.+)`, arg); matched {
			re := regexp.MustCompile(`^--output=(.+)`)
			matches := re.FindStringSubmatch(arg)
			if len(matches) > 1 {
				lokiPushURL = matches[1]
			}
		} else if matched, _ := regexp.MatchString(`^--input=(.+)`, arg); matched {
			re := regexp.MustCompile(`^--input=(.+)`)
			matches := re.FindStringSubmatch(arg)
			if len(matches) > 1 {
				openSearchURL = matches[1]
			}

		} else if matched, _ := regexp.MatchString(`^--index=(.+)`, arg); matched {
			re := regexp.MustCompile(`^--index=(.+)`)
			matches := re.FindStringSubmatch(arg)
			if len(matches) > 1 {
				openSearchIndex = matches[1]
			}
		} else if strings.HasPrefix(arg, "-") {
			fmt.Printf("Error: Unknown argument: %s\n", arg)
			return
		} else {
			fmt.Println("Error: wrong format")
			return
		}
	}

	osClient, err := opensearch.NewClient(opensearch.Config{

		Addresses: []string{openSearchURL},
	})
	if err != nil {
		log.Fatalf("Error creating OpenSearch client: %v", err)
	}
	fmt.Println("Connected to OpenSearch.")

	log.Printf("Starting to query logs from OpenSearch index: %s", openSearchIndex)
	processedCount := 0
	err = queryAndPushLogs(osClient, openSearchIndex, func(logDoc map[string]interface{}) error {
		if err := pushLogToLoki(lokiPushURL, logDoc, openSearchIndex); err != nil {
			return fmt.Errorf("failed to push log to Loki: %w", err)
		}
		processedCount++
		return nil
	})

	if err != nil {
		log.Fatalf("Error during log transfer: %v", err)
	}
	log.Printf("Finished. Successfully processed %d logs from OpenSearch to Loki.", processedCount)
}

func queryAndPushLogs(osClient *opensearch.Client, index string, callback func(logDoc map[string]interface{}) error) error {
	ctx := context.Background()
	var scrollID string
	defer func() {
		if scrollID != "" {
			log.Printf("Clearing OpenSearch scroll ID: %s", scrollID)
			clearScrollReq := opensearchapi.ClearScrollRequest{
				ScrollID: []string{scrollID},
			}
			clearResp, err := clearScrollReq.Do(ctx, osClient)
			if err != nil {
				log.Printf("Warning: Failed to clear OpenSearch scroll: %v", err)
			} else {
				clearResp.Body.Close()
			}
		}
	}()

	initialQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"sort": []map[string]string{
			{"_doc": "asc"},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(initialQuery); err != nil {
		return fmt.Errorf("error encoding initial query: %w", err)
	}

	sizePtr := new(int)
	*sizePtr = scrollBatchSize

	searchReq := opensearchapi.SearchRequest{
		Index:  []string{index},
		Scroll: scrollDuration,
		Size:   sizePtr,
		Body:   &buf,
	}

	res, err := searchReq.Do(ctx, osClient)
	if err != nil {
		return fmt.Errorf("error performing initial OpenSearch search: %w", err)
	}

	for {
		if res.IsError() {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				res.Body.Close()
				return fmt.Errorf("error parsing OpenSearch error response: %w", err)
			}
			res.Body.Close()
			return fmt.Errorf("OpenSearch search error [%s]: %s", res.Status(), e["error"].(map[string]interface{})["reason"])
		}

		var searchResponse map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
			res.Body.Close()
			return fmt.Errorf("error decoding OpenSearch search response: %w", err)
		}
		res.Body.Close()

		hits, ok := searchResponse["hits"].(map[string]interface{})["hits"].([]interface{})
		if !ok || len(hits) == 0 {
			break
		}

		scrollID, ok = searchResponse["_scroll_id"].(string)
		if !ok || scrollID == "" {
			return fmt.Errorf("missing _scroll_id in OpenSearch response")
		}

		for _, hit := range hits {
			doc, ok := hit.(map[string]interface{})["_source"].(map[string]interface{})
			if !ok {
				log.Printf("Warning: Could not parse _source from hit: %v", hit)
				continue
			}
			if err := callback(doc); err != nil {
				return fmt.Errorf("callback failed for document: %w", err)
			}
		}

		scrollReq := opensearchapi.ScrollRequest{
			Scroll:   scrollDuration,
			ScrollID: scrollID,
		}
		res, err = scrollReq.Do(ctx, osClient)
		if err != nil {
			return fmt.Errorf("error performing OpenSearch scroll request: %w", err)
		}
	}

	return nil
}

func pushLogToLoki(lokiURL string, logDoc map[string]interface{}, openSearchIndex string) error {
	timestampStr, ok := logDoc["timestamp"].(string)
	if !ok {
		return fmt.Errorf("log document missing or invalid 'timestamp' field: %v", logDoc)
	}

	// Parse the timestamp. Graylog/OpenSearch often use RFC3339 or similar.
	// Your timestamp is "2024-02-14 20:30:55.410". This looks like a custom format,
	// or time.RFC3339Nano might not exactly match the space and 3 decimal places.
	// Let's adjust the parsing format to match "2006-01-02 15:04:05.000" (Go's reference time format)
	const graylogTimestampFormat = "2006-01-02 15:04:05.000"
	t, err := time.Parse(graylogTimestampFormat, timestampStr)
	if err != nil {
		// If the specific format doesn't work, try RFC3339Nano as a fallback or a more flexible parser
		parsedTime, parseErr := time.Parse(time.RFC3339Nano, timestampStr)
		if parseErr != nil {
			return fmt.Errorf("failed to parse timestamp '%s' with both custom format and RFC3339Nano: %w", timestampStr, parseErr)
		}
		t = parsedTime
		log.Printf("Warning: Timestamp '%s' did not match custom format, but parsed successfully with RFC3339Nano.", timestampStr)
	}

	timestampNanos := strconv.FormatInt(t.UnixNano(), 10)

	message, ok := logDoc["message"].(string)
	if !ok {
		jsonMsg, _ := json.Marshal(logDoc)
		message = string(jsonMsg)
		log.Printf("Warning: 'message' field not found in log, sending full JSON doc as message: %s", message)
	}

	labels := map[string]string{
		"app":          "graylog-forwarder",
		"source_index": openSearchIndex,
		"data_origin":  "historical",
	}

	if app, ok := logDoc["app"].(string); ok {
		labels["app_name"] = app
	}

	if level, ok := logDoc["level"].(float64); ok {
		labels["log_level"] = convertGraylogLevel(int(level))
	} else if levelStr, ok := logDoc["level"].(string); ok {
		labels["log_level"] = strings.ToLower(levelStr)
	}

	if host, ok := logDoc["host"].(string); ok {
		labels["host"] = host
	}

	entry := []string{timestampNanos, message}

	stream := LokiStream{
		Stream: labels,
		Values: [][]string{entry},
	}

	pushRequest := LokiPushRequest{
		Streams: []LokiStream{stream},
	}

	jsonBytes, err := json.Marshal(pushRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal Loki JSON: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, lokiURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request to Loki: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request to Loki: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var respBody bytes.Buffer
		respBody.ReadFrom(resp.Body)
		return fmt.Errorf("Loki returned non-200/204 status: %d - %s", resp.StatusCode, respBody.String())
	}

	return nil
}

func convertGraylogLevel(level int) string {
	switch level {
	case 0:
		return "emergency"
	case 1:
		return "alert"
	case 2:
		return "critical"
	case 3:
		return "error"
	case 4:
		return "warning"
	case 5:
		return "notice"
	case 6:
		return "info"
	case 7:
		return "debug"
	default:
		return "unknown"
	}
}
