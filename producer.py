import pandas as pd
import random
import json
import time
from kafka import KafkaProducer

class Rand_Customer_Producer:

    '''
        bootstrap_servers - bootstap server name 
        sample_range - Range of number of customer need to drwan from total set
        radius  - distance of the customer from shop 
                [0 - 2] - with in mall
                [3 - 5] - very near to mall
                [6 - 7] - near to mall
                [8 - 10] - away from mall
        total_customers - total set we wanted to consider
    '''

    def __init__(self,bootstrap_servers,sample_range,radius,total_customers):
        self.sample_range = sample_range
        self.radius = radius
        self.total_customers = total_customers
        self. producer = self._get_kafka_producer(bootstrap_servers)

    def _get_kafka_producer(self,bootstrap_servers):
        # create kafka producer with json value serializer 
        serializer = lambda v: json.dumps(v).encode('utf-8')
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer= serializer)
        print("Producer running")
        return producer


    def _generateRamdomCustomer(self,sample_size=10,radius=10,total_size=200):
        
        cust_list = []
        #Create total setof customers
        total_set = range(1,total_size)

        #select random n customer from total
        selected_cust = random.sample(total_set,sample_size)

        #add distance to customer
        for i in selected_cust:
            cust_list.append((i,random.randrange(0,radius)))
        
        return cust_list

    def generate_and_publish_msg(self,topic_name):
        # pick the number of custer we need to draw from the range given
        size=random.choice(self.sample_range)
        
        #generate customer list with distance
        customers = self._generateRamdomCustomer(sample_size=size,radius= self.radius, total_size =self.total_customers)
        
        # publish the customer with distance in kafka topic
        for customer in customers:
            self.producer.send( topic_name, {'id': customer[0], 'rad': customer[1]})
        self.producer.flush()

    def send_msg_in_loop(self, topic_name, interval=1, times=30):
        # iterate for every 1 min (Interval) for next 30 times
        for i in range(0,30):
            print(f'publishing for ->{i} iteration')
            self.generate_and_publish_msg(topic_name)
            time.sleep(interval*60) # 

        

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    topic_name = "mymall_cust_topic"
    total_customers = 200
    sample_range = range(40, 50)
    radius = 10

    producer = Rand_Customer_Producer(bootstrap_servers=bootstrap_servers,
             sample_range=sample_range,
             radius=radius,
             total_customers=total_customers)
    # time in mins
    producer.send_msg_in_loop(topic_name, 1 , 25)
    