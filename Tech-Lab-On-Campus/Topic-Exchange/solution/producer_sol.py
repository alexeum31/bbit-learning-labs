import pika
import os
from sys import argv
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self,routing_key,exchange_name):
        self.routing_key, self.exchange_name = routing_key, exchange_name
        self.setupRMQConnection()
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = self.connection.channel()
        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message,
        )
        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()
