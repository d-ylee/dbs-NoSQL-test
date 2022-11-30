package main

// DBS NoSQL Test server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var DB *mongo.Client
var fls []FileLumi

// FileLumi represents file lumi structure used in File structure of BulkBlocks structure
type FileLumi struct {
	LumiSectionNumber int64 `json:"lumi_section_num" bson:"lumi_sectionnum"`
	RunNumber         int64 `json:"run_num" bson:"run_num"`
	EventCount        int64 `json:"event_count" bson:"event_count"`
}

func main() {
	// generate json file from database dump if it does not exist
	if _, err := os.Stat("./fileLumiData.json"); errors.Is(err, os.ErrNotExist) {
		fls = readFileLumiDataDump()
	}

	// setup mongoDB client
	URI := "mongodb://localhost:27017"

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(URI))
	if err != nil {
		log.Fatal(err)
	}
	DB = client

	// setup HTTP server
	r := mux.NewRouter()
	r.HandleFunc("/", FileLumiHandler)
	http.Handle("/", r)

	srv := &http.Server{
		Handler:      r,
		Addr:         "127.0.0.1:8000",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	fmt.Println("Starting server...")
	log.Fatal(srv.ListenAndServe())
}

// FileLumiHandler handles bulk file lumi insert into MongoDB
func FileLumiHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "GET" {
		data, err := json.Marshal(fls)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		fmt.Println("Banana")
		w.Write(data)
	} else if r.Method == "POST" {
		var fl []FileLumi
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = json.Unmarshal(data, &fl)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		flInterface := make([]interface{}, len(fl))
		for i := range fl {
			flInterface[i] = fl[i]
		}

		coll := DB.Database("FileLumis").Collection("fileLumis")
		result, err := coll.InsertMany(context.TODO(), flInterface)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Printf("%v+\n", result)
	}
}

// converts an array of fileLumi csv row string to integers
func strToIArray(arr []string) []int64 {
	t2 := make([]int64, len(arr))

	for idx, i := range arr {
		if idx == 3 {
			continue
		}
		j, err := strconv.ParseInt(i, 10, 64)
		if err != nil {
			panic(err)
		}
		t2[idx] = j
	}
	return t2
}

// read a FileLumis database table dump
func readFileLumiDataDump() []FileLumi {
	var fls []FileLumi

	f, err := os.Open("./1mil")
	if err != nil {
		log.Fatal("Cannot open file")
	}
	csvReader := csv.NewReader(f)
	for {
		data, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal("Cannot parse CSV")
		}

		dataI := strToIArray(data)

		fl := FileLumi{
			LumiSectionNumber: dataI[1],
			RunNumber:         dataI[2],
		}
		fls = append(fls, fl)
	}

	dataJ, err := json.MarshalIndent(fls, "", " ")
	if err != nil {
		panic("Cannot convert to JSON")
	}

	_ = os.WriteFile("./fileLumiData.json", dataJ, os.ModePerm)

	return fls
}
