from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            json_data = json.load(f)
            for json_dict in json_data:
                future = message = self.dict_to_binary(json_dict)
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
        log.error('Error while sending data from Producer', exc_info=excp)
    
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf8')
        