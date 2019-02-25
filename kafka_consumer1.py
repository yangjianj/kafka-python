import time
from kafka import KafkaConsumer,OffsetAndMetadata
from kafka import TopicPartition
# 创建一个连接到指定broker的指定topic的consumer
consumer = KafkaConsumer(# sasl_mechanism="PLAIN",
                         # security_protocol='SASL_PLAINTEXT',
                         # sasl_plain_username="admin",
                         # sasl_plain_password="admin-secret123",
                         # consumer_timeout_ms=30,
                         group_id="g_1",
                         enable_auto_commit=True,
                         auto_commit_interval_ms=500,
                         auto_offset_reset="earliest",
                         #auto_offset_reset="latest",
                         bootstrap_servers="172.31.32.39:9092",
                         client_id ='13'
                          )

# consumer对象支持迭代，当队列中没有消息时会一直等待读取
print(consumer.beginning_offsets(consumer.assignment())) #获取当前消费者可消费的偏移量
print(consumer.topics())  #获取主题列表
#print(consumer.partitions_for_topic('1_yyy')) #获取指定主题的分区信息
# print('11111111111111111')
print(consumer.assignment())  #获取当前消费者topic，分区信息
#consumer.seek(TopicPartition(topic='1_yyy',partition=0),5)#重置偏移量，从第x个偏移量消费
#print(consumer.committed(TopicPartition(topic='1_yyy',partition=0)))

# print(consumer.assignment())
consumer.assign([TopicPartition("test33333",0)])  #指定消费者topic与partition（与consumer定义参数topic互斥）
# consumer.seek(TopicPartition(topic='test33333',partition=0),5)#（要与assign方法一起用）
for i in consumer:
    print("fetching...")
    #print("已提交偏移量",consumer.committed(TopicPartition(topic='test33333', partition=0)))  # 获取当前Group已提交的偏移量
    print(i.topic,i.partition,i.offset,i.value)
    time.sleep(1)

