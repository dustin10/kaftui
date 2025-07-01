#!/bin/sh

docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic kaftui --partitions 2
