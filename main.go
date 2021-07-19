package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

var Session *gocql.Session

//ctx := context.Background()

type Event struct {
	ID       string `json:"id"`
	Text     string `json:"text"`
	Timeline string `json:"timeline"`
}

func homeLink(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome home!")
}

func main() {
	defer Session.Close()
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homeLink)
	router.HandleFunc("/createtweet", createTweet).Methods("POST")
	router.HandleFunc("/getsingletweet/{id}/{text}", getSingleTweet).Methods("GET")
	router.HandleFunc("/getalltweets/{id}/{text}", getAllTweets).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))
	fmt.Println("started on port: 8080")
}

func init() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
	create index on example.tweet(timeline);
	*/

	var err error
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	// connect to the cluster
	Session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("cassandra init done")
}

func createTweet(w http.ResponseWriter, r *http.Request) {
	var e Event

	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Kindly enter data with the event title and description only in order to update")
	}

	err = json.Unmarshal(reqBody, &e)

	id := e.ID
	text := e.Text

	fmt.Println("event", e.ID)
	// // insert a tweet
	if err := Session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", id, text).Exec(); err != nil {
		log.Fatal(err)
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(e)

	//	var id gocql.UUID
	//	var text string
}

func getSingleTweet(w http.ResponseWriter, r *http.Request) {
	/* Search for a specific set of records whose 'timeline' column matches
	 * the value 'me'. The secondary index that we created earlier will be
	 * used for optimizing the search */
	id := mux.Vars(r)["id"]
	var text string

	if err := Session.Query(`SELECT id, text FROM tweet WHERE id = ? LIMIT 1`,
		id).Consistency(gocql.One).Scan(&id, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", id, text)

	var e Event
	e.ID = id
	e.Text = text
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(e)
}

func getAllTweets(w http.ResponseWriter, r *http.Request) {
	// list all tweets
	id := mux.Vars(r)["id"]
	text := mux.Vars(r)["text"]
	scanner := Session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`,
		"me").Iter().Scanner()
	for scanner.Next() {
		err := scanner.Scan(&id, &text)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Tweet:", id, text)
	}
	// scanner.Err() closes the iterator, so scanner nor iter should be used afterwards.
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// Tweet: cad53821-3731-11eb-971c-708bcdaada84 hello world
//
// Tweet: cad53821-3731-11eb-971c-708bcdaada84 hello world
// Tweet: d577ab85-3731-11eb-81eb-708bcdaada84 hello world
