import time
from kafka import KafkaConsumer,OffsetAndMetadata,TopicPartition

# 创建一个consumer
consumer = KafkaConsumer(
    # consumer_timeout_ms=10000, #未收到新消息时迭代等待的时间，默认无限大
    enable_auto_commit=False, #默认自动提交
    #auto_commit_interval_ms=5000,
    group_id="g_2",  #指定groupid后默认提取消息偏移量为上一次提交的偏移量
    bootstrap_servers="172.31.32.39:9092",
    client_id='11'  #同一groupid下同一时刻只会有一个client消费同一partition消息
)
consumer.assign([TopicPartition("test_yang", 0)])  # 指定消费者topic与partition（与consumer定义参数topic互斥）
#consumer.seek(TopicPartition(topic='test33333',partition=0),170)#手动指定TopicPartition的提取偏移量（要与assign方法一起用）,此方法不会改变提交的偏移量
print(consumer.assignment()) #获取分配给当前consumer的topicpartitions
print(consumer.beginning_offsets({TopicPartition(topic='test33333', partition=0)}))  # 获取指定分区的第一个偏移量
print(consumer.end_offsets({TopicPartition(topic='test33333', partition=0)}))  # 获取指定分区的最后一个偏移量
print(consumer.topics())  # 获取主题列表
print(consumer.partitions_for_topic('test33333')) #获取指定主题的分区信息
print(consumer.committed(TopicPartition(topic='test33333', partition=0)))  #获取当前Group已提交的偏移量
# 手动提交偏移量 offsets格式：{TopicPartition:OffsetAndMetadata(offset_num,None)}
# consumer.commit()  # 同步提交当前偏移量（默认值为已消费offset+1）
consumer.commit(offsets={TopicPartition('topic_yang', 0):OffsetAndMetadata(0,None)}) #同步提交

# re=consumer.commit_async(offsets=None, callback=None)
# print(re.succeeded())

res = consumer.poll(timeout_ms=5000,max_records=10) #非实时获取消息,一次性拉去若干条消息
print(res)

print('111111111111')
for i in res.values():
    a=len(i)
    print(a)
    for j in i:
        pass
        print(j)
        # print(j.value)
#print('message num:',a)
print(consumer.committed(TopicPartition(topic='topic_yang', partition=0)))
# for i in consumer:
#     print("fetching...")
#     print("已提交偏移量",consumer.committed(TopicPartition(topic='test33333', partition=0)))  # 获取当前Group已提交的偏移量
#     print(i.topic,i.partition,i.offset,i.value)
#     time.sleep(2)
