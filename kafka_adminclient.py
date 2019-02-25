from kafka.client_async import KafkaClient
from kafka import KafkaConsumer,TopicPartition
import time
from kafka.admin import KafkaAdminClient,NewTopic
consumer = KafkaConsumer(bootstrap_servers="172.31.32.39:9092",
                          client_id ='12'
                           )
print(consumer.topics())  # 获取主题列表
print(consumer.partitions_for_topic('test33333')) #获取指定主题的分区信息

adminclient = KafkaAdminClient(bootstrap_servers="172.31.32.39:9092",
                         client_id ='12'
                          )
adminclient.list_consumer_groups()
newtopic1 = NewTopic(name='creat_test', num_partitions=2, replication_factor=1)
adminclient.create_topics([newtopic1],timeout_ms=5000)
#adminclient.create_partitions({'test33333':(2,1)})
