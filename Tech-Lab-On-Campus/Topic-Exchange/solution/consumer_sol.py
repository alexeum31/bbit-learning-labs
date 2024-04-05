import pika
from consumer_interface import mqConsumerInterface
import os
import time
from concurrent.futures import ThreadPoolExecutor

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.routing_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
        

    def setupRMQConnection(self) :
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.m_connection = pika.BlockingConnection(parameters=conParams)
        self.m_channel = self.m_connection.channel()

        # Establish Channel
        self.m_channel.exchange_declare(self.exchange_name, exchange_type="topic")
        
        #Create the queue if not already present
        self.m_queue_name = self.queue_name
        self.m_channel.queue_declare(queue=self.m_queue_name)
        self.m_channel.queue_bind(queue=self.m_queue_name, routing_key=self.routing_key, exchange=self.exchange_name)
        self.m_channel.basic_consume(self.m_queue_name, self.on_message_callback)


    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ):
        
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag)

        #Print message (The message is contained in the body parameter variable)
        print(f"Incoming Data. Method_Frame:{method_frame}\nHeader_Frame:{header_frame}\nBody:{body}")
        
        pass


    def startConsuming(self):
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")
        # Start consuming messages
        self.m_channel.start_consuming()

        #self.m_pool.submit(self.consumeBlock)
        pass

    def __del__(self):
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.m_channel.stop_consuming()
        # Close Connection
        self.m_pool.shutdown()
        
        pass
