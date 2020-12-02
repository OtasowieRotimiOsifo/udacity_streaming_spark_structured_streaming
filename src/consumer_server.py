#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 19:26:46 2020

@author: otasowie
"""
import asyncio
import logging

from confluent_kafka import Consumer


logger = logging.getLogger(__name__)
logger.setLevel(10)

async def consume(topic_name):
    """Asynchronously consumes data from kafka topic"""
    group_id = "0"
    broker_properties = {
        'bootstrap.servers': "PLAINTEXT://localhost:9092",
        "group.id": group_id,
        'default.topic.config': {'auto.offset.reset': 'earliest'}
    }
    
    topics = [topic_name]
    consumer = Consumer(broker_properties)
    consumer.subscribe(topics)
    #print("and here")
    while True:
        #print("and here2")
        message = consumer.poll(1.0)
        #print(message)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            #print("and here4")
            try:
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(0.1)

    

def close():
     """Cleans up any open kafka consumers"""
     #consumer.close()

async def consumer_handler(topic_name):
    #print("got here")
    t = asyncio.create_task(consume(topic_name))
    await t

def main():
    """Checks for topic and creates the topic if it does not exist"""
    #print("Started")
    try:
        asyncio.run(consumer_handler("sf.crime.statistics.topic"))
    except KeyboardInterrupt as e:
        print("shutting down")
        
if __name__ == "__main__":
    main()
    
    