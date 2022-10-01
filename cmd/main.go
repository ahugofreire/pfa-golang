package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ahugofreire/pfa-go/internal/order/infra/database"
	"github.com/ahugofreire/pfa-go/internal/order/usecase"
	"github.com/ahugofreire/pfa-go/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"net/http"
	"sync"
	"time"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(mysql:3306)/orders")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := database.NewOrderRepository(db)
	useCase := usecase.NewCalculateFinalPriceUseCase(repository)

	http.HandleFunc("/total", func(w http.ResponseWriter, r *http.Request) {
		useCase := usecase.NewGetTotalUseCase(repository)
		output, err := useCase.Execute()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(output)
	})

	go http.ListenAndServe(":8181", nil)

	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	out := make(chan amqp.Delivery)

	maxWorkers := 3
	wg := sync.WaitGroup{}
	go rabbitmq.Consume(ch, out)
	wg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		defer wg.Done()
		go worker(out, useCase, i)
	}
	wg.Wait()
}

func worker(deliveryMessage <-chan amqp.Delivery, useCase *usecase.CalculateFinalPriceUseCase, workerID int) {
	for msg := range deliveryMessage {
		var input usecase.OrderInputDTO
		err := json.Unmarshal(msg.Body, &input)
		if err != nil {
			panic(err)
		}
		input.Tax = 10.0
		_, err = useCase.Execute(input)
		if err != nil {
			fmt.Println("Error unmarshalling message", err)
		}
		msg.Ack(false)
		fmt.Println("Worker", workerID, "processed order", input.ID)
		time.Sleep(1 * time.Second)
	}
}
