from kafka import KafkaConsumer,OffsetAndMetadata,TopicPartition
from kafka.errors import KafkaError
import time
class ConsumerOperate():
    '''
    提供consumer操作
    '''
    def __init__(self,config):
        self.consumer = None
        self.topic=config['topic']
        self.partition=config['partition']
        self.group_id=config['group_id']
        self.client_id=config['client_id']
        self.bootstrap_servers=config['bootstrap_servers']
        self.consumer_timeout_ms=float('inf')
        self._create_kafka_consumer()

    def _create_kafka_consumer(self):
        topic_partition = TopicPartition(topic=self.topic, partition=self.partition)
        try:
            self.consumer = KafkaConsumer(group_id=self.group_id,  # 组信息,没有组信息不能提交偏移量
                                          client_id=self.client_id,
                                          bootstrap_servers=self.bootstrap_servers,
                                          consumer_timeout_ms=self.consumer_timeout_ms
                                          )
            self.consumer.assign([topic_partition])
        except KafkaError as err:
            print(err)

    def get_some_message(self,num,start_offset=None):
        #start_offset:         为None则以该组在kafka server上的偏移量开始拉去
        #num:          需要获取的消息量
        #groupid :             consumer组信息
        #client_id:            consumer client信息
        #topic:                消费的topic
        #partition             消费的partition
        #bootstrap_servers：   kafka服务器信息（localhost:9092）
        topic_partition = TopicPartition(topic=self.topic, partition=self.partition)
        self._create_kafka_consumer()
        if start_offset != None:
            self.consumer.seek(topic_partition, start_offset)
            self.consumer.commit(
                offsets={topic_partition: OffsetAndMetadata(start_offset, None)})
        res = self.consumer.poll(timeout_ms=5000, max_records=num)
        if res != {}:
            for i in res.values():
                for j in i:
                    print(j)
                    self.message.append(j.value)
            a = len(i)
            old_offset = self.consumer.committed(topic_partition)
            print(old_offset)
            new_offset = a + old_offset
            self.consumer.commit(
            offsets={topic_partition: OffsetAndMetadata(new_offset, None)})
        else:
            a=0
        return dict({'message_num':a,'message_list':self.message})

    def set_consumer_timeout(self,timeout=float('inf')):
        # timeout:              未获取新消息的超时时间
        self.consumer_timeout_ms =timeout
        self._create_kafka_consumer()

    def commit_offset(self):
        self.consumer.commit()

    def reset_consumer_group_and_client(self,groupid,clientid):
        self.group_id=groupid
        self.client_id=clientid
        self._create_kafka_consumer()

    def reset_consumer_server_topic_and_partition(self,server,topic,partition):
        self.bootstrap_servers=server
        self.topic=topic
        self.partition=partition
        self._create_kafka_consumer()


if __name__ == "__main__":
    config={'group_id':'g_2','client_id':12,'topic':'topic_yang','partition':0,'bootstrap_servers':'172.31.32.39:9092'}
    con=ConsumerOperate(config)
    con.set_consumer_timeout(20000)
    print(con.consumer.committed(TopicPartition(topic='test33333', partition=0)))

    for i in con.consumer:
        print("fetching")
        print(i.topic,i.offset)
        con.commit_offset()
        print(con.consumer.committed(TopicPartition(topic='topic_yang', partition=0)))

