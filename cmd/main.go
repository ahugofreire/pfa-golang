package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ahugofreire/pfa-go/internal/order/infra/database"
	"github.com/ahugofreire/pfa-go/internal/order/usecase"
	"github.com/ahugofreire/pfa-go/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(mysql:3306)/orders")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := database.NewOrderRepository(db)
	useCase := usecase.NewCalculateFinalPriceUseCase(repository)
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

	//go worker(out, useCase, 1)
	//go worker(out, useCase, 2)
	//worker(out, useCase, 3)

	//inputDto := usecase.OrderInputDTO{
	//	ID:    "1234567",
	//	Price: 100,
	//	Tax:   10,
	//}
	//
	//output, err := useCase.Execute(inputDto)
	//if err != nil {
	//	panic(err)
	//}
	//println(output.FinalPrice)

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
	}
}
