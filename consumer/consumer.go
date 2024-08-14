package main

import (
    "context"
    "encoding/json"
    "log"
    "github.com/segmentio/kafka-go"
)

type Message struct {
    UserID    string `json:"user_id"`
    Message   string `json:"message"`
    Timestamp int64  `json:"timestamp"`
}

func main() {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{"localhost:9092"},
        Topic:   "go-kafka-topic",
        GroupID: "go-kafka-group",
    })

    for {
        msg, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Fatalln(err)
        }

        var message Message
        if err := json.Unmarshal(msg.Value, &message); err != nil {
            log.Fatalln(err)
        }

        log.Printf("Xabar o'qildi: UserID=%s, Message=%s, Timestamp=%d\n", message.UserID, message.Message, message.Timestamp)
    }
}
