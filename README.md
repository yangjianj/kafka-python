# kafka-python-introduction

#kafka-python使用介绍  
#1.producer向服务器发送服务器不存在的topic消息时会在服务器新建topic默认partition为0  
#2.Producer发送消息后需要等待服务器响应是否成功（feature.get(timeout=10)，否则多个消息过快连续发送会出现不成功的现象  
#3.发送消息给指定topic的指定partition（partition不为0则需要先在broker上创建对应partition）  
#4.consumer对象不指定groupid则不能提交偏移量  
#5.同一时刻同一groupid下只会有一个client消费同一partition消息  
