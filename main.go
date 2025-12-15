package main

import (
    "context"
    "fmt"
    "net/http"
    "os"
    "strings"
    "time"
    
    "cloud.google.com/go/bigquery"
    "github.com/GoogleCloudPlatform/functions-framework-go/functions"
    twiml "github.com/twilio/twilio-go/twiml" 
)

const (
    bigQueryDatasetEnv = "BIGQUERY_DATASET"
    bigQueryTableEnv   = "BIGQUERY_TABLE"
)

var (
    bqClient *bigquery.Client
    datasetID string
    tableID   string
)

func init() {
    functions.HTTP("HandleTwilioSms", HandleTwilioSms)

    datasetID = os.Getenv(bigQueryDatasetEnv)
    tableID = os.Getenv(bigQueryTableEnv)
    if datasetID == "" || tableID == "" {
        fmt.Printf("Warning: Missing environment variables %s or %s. BigQuery logging will fail.\n", bigQueryDatasetEnv, bigQueryTableEnv)
        return
    }
    
    var err error
    bqClient, err = bigquery.NewClient(context.Background(), os.Getenv("GCP_PROJECT")) 
    if err != nil {
        fmt.Printf("Error initializing BigQuery client: %v\n", err)
    }
}

type LogEntry struct {
    Timestamp   time.Time `bigquery:"timestamp"`
    FromNumber  string    `bigquery:"from_number"`
    MessageBody string    `bigquery:"message_body"`
}

func HandleTwilioSms(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    if err := r.ParseForm(); err != nil {
        http.Error(w, "Failed to parse form data", http.StatusBadRequest)
        return
    }

    incomingBody := r.PostForm.Get("Body")
    fromNumber := r.PostForm.Get("From")

    if incomingBody != "" && strings.Contains(strings.ToLower(incomingBody), "stop") {
        if err := streamToBigQuery(ctx, fromNumber, incomingBody); err != nil {
            fmt.Printf("BigQuery Streaming Error: %v\n", err)
        }
    }

    response := twiml.NewMessagingResponse()
    
    message := response.Message
    message.Body = fmt.Sprintf("Your message from %s has been processed by Go.", fromNumber)

    twimlBytes, err := twiml.String(response)
    if err != nil {
        http.Error(w, "Failed to generate TwiML", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/xml")
    w.WriteHeader(http.StatusOK)
    w.Write([]byte(twimlBytes))
}

func streamToBigQuery(ctx context.Context, fromNumber, incomingBody string) error {
    if bqClient == nil {
        return fmt.Errorf("BigQuery client not initialized")
    }

    u := bqClient.Dataset(datasetID).Table(tableID).Uploader()

    row := LogEntry{
        Timestamp:   time.Now().UTC(),
        FromNumber:  fromNumber,
        MessageBody: incomingBody,
    }

    if err := u.Put(ctx, []*LogEntry{&row}); err != nil {
        if merr, ok := err.(bigquery.MultiError); ok {
            for _, err := range merr {
                fmt.Printf("BigQuery Row Error: %v\n", err.Error())
            }
        }
        return fmt.Errorf("failed to stream row to BigQuery: %w", err)
    }

    fmt.Printf("Successfully inserted row into %s.%s\n", datasetID, tableID)
    return nil
}
