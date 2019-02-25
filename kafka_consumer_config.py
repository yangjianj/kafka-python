import time
from kafka import KafkaConsumer
from kafka import TopicPartition

# 创建一个连接到指定broker的指定topic的consumer
consumer = KafkaConsumer(
    # sasl_mechanism="PLAIN",
    # security_protocol='SASL_PLAINTEXT',
    # sasl_plain_username="admin",
    # sasl_plain_password="admin-secret123",
    consumer_timeout_ms=10000, #未收到新消息时迭代等待的时间
    enable_auto_commit=True,    #自动提交偏移量，含有组信息才生效
    auto_commit_interval_ms=500,#自动提交周期
    auto_offset_reset='latest',#"latest",#没有组信息时从topic最末尾消费（默认latest）,earliest则从topic最早开始消费
    group_id="g_2",  # 组信息,没有组信息不能提交偏移量
    bootstrap_servers="172.31.32.39:9092",
    client_id='12'
)
consumer.assign([TopicPartition("test33333", 0)])  # 指定消费者topic与partition（与consumer定义参数topic互斥）
# consumer.seek(TopicPartition(topic='test33333',partition=0),170)#手动指定TopicPartition的提取偏移量（要与assign方法一起用）

for i in consumer:
    #print("偏移量：", consumer.committed(TopicPartition(topic='test33333', partition=0)))  # 获取指定topic,partition的偏移量
    print("fetching...")
    print(i.topic,i.partition,i.offset,i.value)
    time.sleep(2)
