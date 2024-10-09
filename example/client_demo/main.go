package main

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/xaionaro-go/go-rtmp"
	rtmpmsg "github.com/xaionaro-go/go-rtmp/message"
)

const (
	chunkSize = 128
)

func main() {
	client, err := rtmp.Dial("rtmp", "localhost:1935", &rtmp.ConnConfig{
		Logger: log.StandardLogger(),
	})
	if err != nil {
		log.Fatalf("Failed to dial: %+v", err)
	}
	defer client.Close()
	log.Infof("Client created")

	ctx := context.Background()

	if err := client.Connect(ctx, nil); err != nil {
		log.Fatalf("Failed to connect: Err=%+v", err)
	}
	log.Infof("connected")

	stream, err := client.CreateStream(ctx, nil, chunkSize)
	if err != nil {
		log.Fatalf("Failed to create stream: Err=%+v", err)
	}
	defer stream.Close()

	if err := stream.Publish(ctx, &rtmpmsg.NetStreamPublish{
		PublishingName: "testtesttesttest",
		PublishingType: "live",
	}); err != nil {
		log.Fatalf("Failed to send publish message: Err=%+v", err)
	}

	log.Infof("stream created")
}
