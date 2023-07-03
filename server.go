package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
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

func main() {
	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "ping")
	})

	e.POST("/users", func(c echo.Context) error {
		connectionString := "couchbase://" + os.Getenv("COUCHBASE_HOST")
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
			return c.String(http.StatusInternalServerError, "connect "+err.Error())
		}
		log.Println("Connected to Couchbase:"+connectionString, bucketName, username, password)
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

	e.Logger.Fatal(e.Start(":3000"))
}
