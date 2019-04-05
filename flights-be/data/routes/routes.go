package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
	signer "github.com/edoardo849/apex-aws-signer"
	"github.com/olivere/elastic"
	"io/ioutil"
	"net/http"
	"os"
)

type Route struct {
	ArrivalDateTime   string `json:"arrivalDateTime"`
	DepartureDateTime string `json:"departureDateTime"`
	Duration          string `json:"duration"`
	FlightType        string `json:"flightType"`
	StartAirport      string `json:"startAirport"`
	FinalAirport      string `json:"finalAirport"`
	Flights           []struct {
		ArrivalAirportIataCode   string `json:"arrivalAirportIataCode"`
		ArrivalDateTime          string `json:"arrivalDateTime"`
		DepartureAirportIataCode string `json:"departureAirportIataCode"`
		DepartureDateTime        string `json:"departureDateTime"`
	} `json:"flights"`
}

func loadFilesFromDir(root string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(root)
	if err != nil {
		return files, err
	}

	for _, file := range fileInfo {
		files = append(files, root+"/"+file.Name())
	}
	return files, nil
}

func loadFile(client *elastic.Client, file string) {
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	var items []Route
	err = json.Unmarshal(raw, &items)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	persistItemsOnEs(client, items)
}

func buildClient() (*elastic.Client, error) {
	transport := signer.NewTransport(session.New(&aws.Config{Region: aws.String("eu-west-1")}), elasticsearchservice.ServiceName)

	httpClient := &http.Client{
		Transport: transport,
	}

	return elastic.NewClient(
		elastic.SetURL("https://search-graphql-wroclaw-es-u4yx5su6ng6cxiom4eapqpz2im.eu-west-1.es.amazonaws.com"),
		elastic.SetScheme("https"),
		elastic.SetHttpClient(httpClient),
		elastic.SetSniff(false), // See note below
	)
}

func persistItemsOnEs(client *elastic.Client, items []Route) {
	bulk := client.Bulk()
	for _, item := range items {
		if len(item.Flights) == 1 {
			item.FlightType = "DIRECT"
		} else {
			item.FlightType = "CONNECTING_FLIGHT"
		}
		item.StartAirport = item.Flights[0].DepartureAirportIataCode
		item.FinalAirport = item.Flights[len(item.Flights)-1].ArrivalAirportIataCode

		req := elastic.NewBulkIndexRequest().
			Index("routes").
			Type("route").
			Doc(item)

		bulk.Add(req)
	}

	response, err := bulk.Do(context.Background())
	if err != nil {
		fmt.Println("Got error on uploading data:")
		fmt.Println(err.Error())
		os.Exit(1)
	} else {
		fmt.Printf("Successfully uploaded: %v\n", response)
	}
}

func main() {
	client, err := buildClient()
	if err != nil {
		fmt.Println("Got error marshalling map:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	filesFromDir, err := loadFilesFromDir("./routes")
	if err != nil {
		fmt.Println("Couldn't load files:")
		fmt.Println(err.Error())
		os.Exit(1)
	}
	for _, file := range filesFromDir {
		loadFile(client, file)
	}
}
