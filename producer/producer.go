package main

import (
    "context"
    "encoding/json"
    "log"
    "time"
    "github.com/segmentio/kafka-go"
)

type Message struct {
    UserID    string `json:"user_id"`
    Message   string `json:"message"`
    Timestamp int64  `json:"timestamp"`
}

func main() {
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "go-kafka-topic",
    })

    msg := Message{
        UserID:    "user123",
        Message:   "Salom, Kafka!",
        Timestamp: time.Now().Unix(),
    }

    msgBytes, _ := json.Marshal(msg)
    err := writer.WriteMessages(context.Background(),
        kafka.Message{
            Key:   []byte(msg.UserID),
            Value: msgBytes,
        },
    )
    if err != nil {
        log.Fatalln(err)
    }

    log.Println("Xabar yuborildi:", string(msgBytes))
    writer.Close()
}
