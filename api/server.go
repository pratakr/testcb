package api

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

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
	ID             string `json:"id"`
	MerchantID     string `json:"merchant_id"`
	Customer       string `json:"customer"`
	OrderNo        string `json:"order_no"`
	PaymentChannel string `json:"payment_channel"`
	PaymentDate    string `json:"payment_date"`
	Amount         int    `json:"amount"`
	Fee            int    `json:"fee"`
	Discount       int    `json:"discount"`
	TotalAmount    int    `json:"total_amount"`
	Currency       string `json:"currency"`
	RouteNo        string `json:"route_no"`
	CardNo         string `json:"card_no"`
	Status         int    `json:"status"`
	IsSattle       string `json:"is_sattle"`
	CreatedBy      string `json:"created_by"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedBy      string `json:"updated_by"`
	UpdatedAt      int64  `json:"updated_at"`
}

type RespData struct {
	Total int       `json:"total"`
	Page  int       `json:"page"`
	Data  []Payment `json:"data"`
}

type Api struct {
}

func (api *Api) Start() {
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
	bucket := cluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		return c.String(http.StatusInternalServerError, "wait "+err.Error())
	}
	log.Println("Connected to Couchbase:"+connectionString, bucketName, username, password)

	//Connect Kafka
	// kafkaConnectionString := os.Getenv("KAFKA_CONNECTION_STRING")
	// config := &kafka.ConfigMap{
	// 	"bootstrap.servers": kafkaConnectionString, // kafka broker addr
	// }
	// topic := "payments" // target topic name
	// // Create Kafka producer
	// producer, err := kafka.NewProducer(config)
	// if err != nil {
	// 	log.Fatal("Failed to create producer: ", err)
	// }
	// log.Println("Kafka connect:"+kafkaConnectionString, topic)

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

	e.GET("/payments", func(c echo.Context) error {
		var respData RespData

		queryStr := fmt.Sprintf("SELECT * FROM `payments` limit 25")
		rows, err := cluster.Query(queryStr, &gocb.QueryOptions{})
		if err != nil {
			return c.String(http.StatusInternalServerError, "query "+err.Error())
		}
		respData.Data = []Payment{}
		var payment Payment
		for rows.Next() {
			rows.Row(&payment)
			respData.Data = append(respData.Data, payment)
			payment = Payment{}
		}

		return c.JSON(http.StatusOK, respData)

	})

	//Couchbase
	e.POST("/payments", func(c echo.Context) error {

		payment := new(Payment)
		c.Bind(&payment)
		id := uuid.New().String()
		payment.ID = id
		//dat, _ := json.Marshal(payment)
		// log.Println("payment_id:", string(dat))

		collection := bucket.Collection("payments")
		binaryC := collection.Binary()
		key := "paymentCounter"
		result, err := binaryC.Increment(key, &gocb.IncrementOptions{
			Initial: 10,
			Delta:   1,
		})
		if err != nil {
			panic(err)
		}

		payment_id := fmt.Sprintf("%d", result.Content())
		payment.ID = payment_id

		_, err = collection.Upsert(id, payment, nil)
		if err != nil {
			return c.String(http.StatusInternalServerError, "insert "+err.Error())
		}
		return c.JSON(http.StatusOK, struct{ ID string }{ID: id})
	})

	//Kafka producer
	// e.POST("/payments", func(c echo.Context) error {

	// 	payment := new(Payment)
	// 	c.Bind(&payment)
	// 	id := uuid.New().String()
	// 	payment.ID = id

	// 	value, _ := json.Marshal(payment)
	// 	err = producer.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Key:            []byte(id),
	// 		Value:          []byte(value),
	// 	}, nil)
	// 	if err != nil {
	// 		return c.String(http.StatusInternalServerError, "producer "+err.Error())
	// 	}
	// 	// producer.Flush(1000)
	// 	return c.JSON(http.StatusOK, struct{ ID string }{ID: id})
	// })

	e.Logger.Fatal(e.Start(":3000"))
}
