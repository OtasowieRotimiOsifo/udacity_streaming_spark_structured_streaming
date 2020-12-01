from kafka import KafkaProducer
import json
import time

import logging

from confluent_kafka.admin import AdminClient, NewTopic
logger = logging.getLogger(__name__)

class ProducerServer(KafkaProducer):

    existing_topics = set(list())
    
    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        
        if self._topic_exists(self.topic) == False:
            print(self.topic)
            ret = self.__create_topic()
            if ret == 1:
                #print(len(self.existing_topics))
                ProducerServer.existing_topics.add(self.topic)

    def generate_data(self):
        with open(self.input_file) as f:
            json_data = json.load(f)
            for json_dict in json_data:
                message = self.dict_to_binary(json_dict)
                # Asynchronous by default
                self.send(topic=self.topic, value=message).\
                add_callback(self.__on_send_success).\
                add_errback(self.__on_send_error)
                
                time.sleep(1)

    def __on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def __on_send_error(excp):
        logger.error('Error while sending data from Producer', exc_info=excp)
    
    def _topic_exists(self, topic_name: str) -> bool:
        if topic_name in ProducerServer.existing_topics:
            return True
        return False
        
    def __create_topic(self) -> int:
        """Creates the producer topic if it does not already exist"""
        try:
            client = AdminClient({"bootstrap.servers": "localhost:9092"})
            topic_metadata = client.list_topics(timeout=200)
            topic_check = self.topic in set(t.topic for t in iter(topic_metadata.topics.values()))
            if  topic_check == True:
                 return -1
             
            futures = client.create_topics(
                [
                    NewTopic(
                        topic=self.topic,
                        num_partitions=3,
                        replication_factor=1
                        #config={
                           # "cleanup.policy": "delete",
                          #  "compression.type": "lz4",
                          #  "delete.retention.ms": "2000",
                         #   "file.delete.delay.ms": "2000",
                        #}
                    )
                ]
            )
            status = 1
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.debug("topic created")  
                except Exception as e:
                    logger.fatal(f"failed to create topic {self.topic}: {e}")
                    status = -1
        except Exception as ex:
            logger.warn(f"Error in create topic {self.topic}: {ex}")
            status = -1
        return status
    
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf8')
        