###最后整合阶段遇到的问题
打包后，运行==》空指针异常
解决：查看日志信息，爆出dateDimmon空指针
1.dataDimmon查看发现反序列化时，没有将数据复制给成员变量，
写代码时直接使用idea的快捷键，导致错误...

错误2：
at com.wangyg.output.MysqlOutputFormat$MysqlRecordWriter.<init>(MysqlOutputFormat.java:85)
解决：
    数据库连接为空==》数据库库名和代码中不同，导致获取连接connection=null


Error: java.lang.UnsupportedOperationException: executeLargeUpdate not implemented
	at java.sql.PreparedStatement.executeLargeUpdate(PreparedStatement.java:1318)
	at com.wangyg.conver.DimensionConvertor.exeSql(DimensionConvertor.java:67)
解决：函数使用错误	executeLargeUpdate, 应使用executeUpdate():

注：
web下的配置文件需要进行更改，需要和自己本地的数据库名和mysql用户密码保持一致，否则出错