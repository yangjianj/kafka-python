import time
from kafka import KafkaConsumer,OffsetAndMetadata,TopicPartition

# 创建一个连接
consumer = KafkaConsumer(
    # consumer_timeout_ms=10000, #未收到新消息时迭代等待的时间
    group_id="g_2",
    bootstrap_servers="172.31.32.39:9092",
    client_id='12'
)
consumer.assign([TopicPartition("test33333", 0)])  # 指定消费者topic与partition（与consumer定义参数topic互斥）
# consumer.seek(TopicPartition(topic='test33333',partition=0),170)#手动指定Topic，Partition的提取偏移量（要与assign方法一起用）
for i in consumer:
    print("fetching...")
    print(i)
    print(i.topic)
    print(i.partition)
    print(i.offset)
    print(i.timestamp)
    print(i.key)
    print(i.value)
