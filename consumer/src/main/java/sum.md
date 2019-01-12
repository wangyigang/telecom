## 问题总结
1. 数据倾斜 ：使用^
2. 空指针异常： table没有初始化  



kafka: server.log
hbase:log 中  


事实表
维度表

星型模型：维度表没有维度表==》星型模型
雪花模型：维度表是否还有维度表：==》有  维度表


将项目完全可运行jar包后，上传到Linux系统中，通过java 命令运行：根据项目特性，需要开启数据方，flume，hbase和运行下面的命令
java -cp consumer-1.0-SNAPSHOT-jar-with-dependencies.jar com.wangyg.consumer.HBaseConsumer

#### 事务隔离级别
读未提交:read uncommited:
读已提交-read committed:
可重复读:reapeted-read:
可串行化:serialize:


事务隔离界别
读未提交：会出现脏读，幻读，不可重复读
度已提交：
可重复读
可串行化：


脏读：脏读就是读取到未提交的数据，A的数据未提交，就被B看到
不可重复读：同一个事物中，前后两次读取的结果不同
幻读：

脏读--不可重复读--幻读 
脏读--不可重复读--幻读



1.客户端有一个主线程，
2.主线程创建一个eventThread()和sendThread()
3.一个负责监听，一个负责网络连接
4.通过connect线程将注册的监听事件发送给zookeeper，
5.在zookeeper的注册监听器列表中将注册的监听事件添加到列表中
6.zookeeper监听有数据或路径发生变化，就将这个消息发送给监听线程
7.listenner线程调用Process()方法
常见监听：监听节点数据的变化
          监听子节点增减的变化
          
          

#scala spark hive sql 




















