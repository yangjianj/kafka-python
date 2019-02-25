import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 创建一个连接到指定broker的Producer
producer = KafkaProducer(bootstrap_servers="172.31.32.39:9092")
for _ in range(10):
    current_time = time.strftime('%Y%m%d-%H:%M:%S', time.localtime())
    message = "test_yang " + str(current_time)
    # 发送一个消息到指定topic
    # python中的数据采用二进制编码，要转换成utf-8编码格式再发出
    # 不指定partition时kafka会自动将这些消息分配到几个partition中
    future=producer.send(topic="topic_yang2",partition=0,value=message.encode("utf-8"))
    # 其定义为def send(topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None)
    try:
        record_data=future.get(timeout=10)  #等待服务器成功接受消息
    except KafkaError:
        print('error')
    print(record_data.topic)
    print(record_data.partition)
    print(record_data.offset)
    print("already sent : ", message)
