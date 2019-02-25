import time
from kafka import KafkaConsumer,TopicPartition ,OffsetAndMetadata

# 创建一个consumer`
consumer = KafkaConsumer(
    #consumer_timeout_ms=10000, #未收到新消息时迭代等待的时间
    fetch_max_wait_ms= 500,
    #enable_auto_commit=True, #默认自动提交
    #auto_commit_interval_ms=5000,
    #auto_offset_reset='earliest',#lastest  组信息为None或新创建的组时（偏移量为None）按此参数提取消息
    group_id="g_2",  #指定groupid后默认提取消息偏移量为上一次提交的偏移量
    bootstrap_servers="172.31.32.39:9092",
    client_id='111'  #同一groupid下同一时刻只会有一个client消费同一partition消息
)
consumer.assign([TopicPartition("topic_yang2", 0)])  # 指定消费者topic与partition（与consumer实例化参数topic互斥）
consumer.seek(TopicPartition(topic='topic_yang2',partition=0),80)#手动指定Topic，Partition的提取偏移量（要与assign方法一起用）
a=0
#print("偏移量：", consumer.committed(TopicPartition(topic='topic_yang2', partition=0)))
for i in consumer:
    print("fetching...")
    #print("偏移量：", consumer.committed(TopicPartition(topic='topic_yang2', partition=0)))  # 获取指定topic,partition的偏移量
    print(i.topic,i.partition,i.offset,i.value)
    print(time.localtime())
    # time.sleep(0.2)
    # a=a+1
    # if a==5:break
# 手动提交偏移量 offsets格式：{TopicPartition:OffsetAndMetadata(offset_num,None)}
    #consumer.commit(offsets={TopicPartition('test33333', 0): OffsetAndMetadata(190, None)})  # 同步提交
   # consumer.commit()  # 同步提交当前偏移量（已消费offset+1）
