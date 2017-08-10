# kettle-plugins

## es-output
  将数据输出到ES中

## orcparquet
  将数据输出到HDFS的Orc文件或者Parquet文件中，并可以以输出文件创建Hive表

## hbase
  hbase-output: 将数据输出到hbase
  hbase-input: 待开发

## increment-etl
  1、增量抽取插件，在job开始的时候使用增量参数设置插件，作业执行完成后后使用增量更新插件。
  2、如果转换的输出中用到了上述的es-output或者orcparquet，则无需使用增量更新步骤。
  3、具体可参考使用说明文档：how-to-use-this-plugin/Kettle增量插件使用说明.docx。