from kafka import KafkaConsumer,OffsetAndMetadata,TopicPartition
from kafka.errors import KafkaError
import time
class ConsumerOperate():
    '''
    提供consumer操作
    '''
    def __init__(self,config):

        self.consumer = None                                #kafka consumer接口
        self.topic=config['topic']                          #消费的topic
        self.partition=config['partition']                  #消费的partition
        self.group_id=config['group_id']                    #consumer组信息
        self.client_id=config['client_id']                  #consumer client信息
        self.bootstrap_servers=config['bootstrap_servers'] #消费的partition
        self.consumer_timeout_ms=float('inf')
        self.message = []

        self.topic_partition = TopicPartition(topic=self.topic, partition=self.partition)

    def create_kafka_consumer(self):

        try:
            self.consumer = KafkaConsumer(group_id=self.group_id,  # 组信息,没有组信息不能提交偏移量
                                          client_id=self.client_id,
                                          bootstrap_servers=self.bootstrap_servers,
                                          consumer_timeout_ms=self.consumer_timeout_ms
                                          )
            self.consumer.assign([self.topic_partition])
        except KafkaError as err:
            print(err)


    def set_consumer_group_id(self, groupid):
        self.group_id = groupid

    def set_consumer_client_id(self, clientid):
        self.client_id = clientid

    def set_consumer_bootstrap_servers(self, server):
        self.bootstrap_servers = server

    def set_comsumer_topic(self, topic):
        self.topic = topic

    def set_consumer_partition(self, partition):
        self.partition = partition

    def set_consumer_timeout(self,timeout=float('inf')):
        # timeout: 未获取新消息的超时时间
        self.consumer_timeout_ms =timeout

    def commit_offset(self):
        self.consumer.commit()


    def get_some_message(self,num,start_offset=None):
        #start_offset:         为None则以该组在kafka server上的偏移量开始拉去
        #message_num:          需要获取的消息量
        #groupid :             consumer组信息
        #client_id:            consumer client信息
        #topic:                消费的topic
        #partition             消费的partition
        #bootstrap_servers：   kafka服务器信息（localhost:9092）
        if start_offset != None:
            self.consumer.seek(self.topic_partition, start_offset)
            self.consumer.commit(
                offsets={self.topic_partition: OffsetAndMetadata(start_offset, None)})
        res = self.consumer.poll(timeout_ms=5000, max_records=num)
        if res != {}:
            for i in res.values():
                for j in i:
                    print(j)
                    self.message.append(j.value)
            a = len(i)
            old_offset = self.consumer.committed(self.topic_partition)
            print(old_offset)
            new_offset = a + old_offset
            self.consumer.commit(
            offsets={self.topic_partition: OffsetAndMetadata(new_offset, None)})
        else:
            a=0
        return dict({'message_num':a,'message_list':self.message})



if __name__ == "__main__":
    config={'start_offset':None,'group_id':'g_2','client_id':12,'topic':'test33333','partition':0,'bootstrap_servers':'172.31.32.39:9092'}

    con=ConsumerOperate(config)
    con.set_consumer_timeout(20*1000)
    con.create_kafka_consumer()

    print(con.consumer.committed(TopicPartition(topic='test33333', partition=0)))#获取当前的偏移量
    print(time.time())
    #con.get_some_message(num=3)

    for i in con.consumer:
        print("fetching")
        print(i.value)
        print(i.topic,i.offset)
        con.commit_offset()
        print(con.consumer.committed(TopicPartition(topic='test33333', partition=0)))#获取当前的偏移量

    print(time.time())