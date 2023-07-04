package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type User struct {
	ID        string `json:"id"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Age       int    `json:"age"`
	Sex       string `json:"sex"`
	Address   string `json:"address"`
	PhoneNo   string `json:"phoneno"`
	Email     string `json:"email"`
	Education string `json:"education"`
}

type Payment struct {
	ID      string `json:"id"`
	Product string `json:"product"`
	Amount  int    `json:"amount"`
	Channel string `json:"channel"`
}

func main() {
	//Connect DB
	connectionString := "couchbase://" + os.Getenv("COUCHBASE_CLUSTER_HOST")
	bucketName := os.Getenv("COUCHBASE_BUCKET_NAME")
	username := os.Getenv("COUCHBASE_USERNAME")
	password := os.Getenv("COUCHBASE_PASSWORD")

	// connectionString := "couchbase://node48137-env-4205298.th1.proen.cloud:11294"
	// bucketName := "paygate"
	// username := "admax"
	// password := "fcD!1234"

	cluster, err := gocb.Connect(connectionString, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		log.Println("connect " + err.Error())
	}
	log.Println("Connected to Couchbase:"+connectionString, bucketName, username, password)

	//Connect Kafka
	kafkaConnectionString := os.Getenv("KAFKA_CONNECTION_STRING")
	config := &kafka.ConfigMap{
		"bootstrap.servers": kafkaConnectionString, // kafka broker addr
	}
	topic := "payments" // target topic name
	// Create Kafka producer
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatal("Failed to create producer: ", err)
	}
	log.Println("Kafka connect:"+kafkaConnectionString, topic)

	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "ping")
	})

	e.POST("/users", func(c echo.Context) error {

		bucket := cluster.Bucket(bucketName)
		err = bucket.WaitUntilReady(5*time.Second, nil)
		if err != nil {
			return c.String(http.StatusInternalServerError, "wait "+err.Error())
		}

		collection := bucket.Collection("users")
		user := new(User)
		c.Bind(&user)
		binaryC := collection.Binary()
		key := "goDevguideExampleCounter"
		result, err := binaryC.Increment(key, &gocb.IncrementOptions{
			Initial: 10,
			Delta:   1,
		})
		if err != nil {
			panic(err)
		}
		id := fmt.Sprintf("%d", result.Content())

		user.ID = id
		_, err = collection.Upsert(id, user, nil)
		if err != nil {
			return c.String(http.StatusInternalServerError, "insert "+err.Error())
		}
		return c.JSON(http.StatusOK, user)
	})

	e.POST("/payments", func(c echo.Context) error {

		payment := new(Payment)
		c.Bind(&payment)
		id := uuid.New().String()
		payment.ID = id

		value, _ := json.Marshal(payment)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(id),
			Value:          []byte(value),
		}, nil)
		if err != nil {
			return c.String(http.StatusInternalServerError, "producer "+err.Error())
		}
		producer.Flush(15 * 1000)
		return c.JSON(http.StatusOK, struct{ ID string }{ID: id})
	})

	e.Logger.Fatal(e.Start(":3000"))
}
