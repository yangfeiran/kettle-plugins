package com.chinacloud.orcparquet.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastBooleanToStringViaLongToString;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.example.data.Group;
import org.pentaho.di.core.exception.KettleFileException;

import com.chinacloud.orcparquet.OrcProxy;

public class OrcFileUtil {
	
	static String ClusterName = "nsstargate";
	private static final String HADOOP_URL = "hdfs://"+ClusterName;
	public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        conf.set("dfs.ha.namenodes."+ClusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn1", "172.16.50.80:8020");
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn2", "172.16.50.81:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider."+ClusterName, 
        		"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

	public static void main(String[] args) throws Exception {
		//writeOrcFile();
		//writeOrcFileLocal();
		dataType();
		//test();
	}
	
	public static void writeOrcFile() throws Exception {
		//定义ORC数据结构，即表结构
		/*CREATE TABLE lxw_orc1 (
		 field1 STRING,
		 field2 STRING,
		 field3 STRING
		) stored AS orc;*/
		TypeDescription schema = TypeDescription.createStruct()
				.addField("field1", TypeDescription.createString())
		        .addField("field2", TypeDescription.createString())
		        .addField("field3", TypeDescription.createString());
		//输出ORC文件到HDFS
		String fileName = "/user/test/orc/test_orc_file0.orc";
		Path path = new Path(fileName);
		FileSystem fs;
		try {
			fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
		    }
		} catch (Exception e) {
			e.printStackTrace();
			throw new KettleFileException(e.getCause());
		}
		Writer writer = OrcFile.createWriter(path,
				OrcFile.writerOptions(conf)
		          .setSchema(schema)
		          .stripeSize(67108864)
		          .bufferSize(131072)
		          .blockSize(134217728)
		          .compress(CompressionKind.ZLIB)
		          .version(OrcFile.Version.V_0_12));
		//要写入的内容
		String[] contents = new String[]{"1,a,哈哈","2,b,中文","3,c,cc","4,d,dd"};
		
		VectorizedRowBatch batch = schema.createRowBatch();
		for(String content : contents) {
			int rowCount = batch.size++;
			String[] logs = content.split(",", -1);
			for(int i=0; i<logs.length; i++) {
				((BytesColumnVector) batch.cols[i]).setVal(rowCount, logs[i].getBytes());
				//batch full
				if (batch.size == batch.getMaxSize()) {
				    writer.addRowBatch(batch);
				    batch.reset();
				}
			}
		}
		writer.addRowBatch(batch);
		writer.close();
	}
	
	/**
	 * 输出ORC文件到本地磁盘上，需要在Linux上执行
	 * @throws Exception
	 */
	public static void writeOrcFileLocal() throws Exception {
		//定义ORC数据结构，即表结构
		/*CREATE TABLE lxw_orc1 (
		 field1 STRING,
		 field2 STRING,
		 field3 STRING
		) stored AS orc;*/
		TypeDescription schema = TypeDescription.createStruct()
				.addField("field1", TypeDescription.createString())
		        .addField("field2", TypeDescription.createString())
		        .addField("field3", TypeDescription.createString());
		//输出ORC文件到本地磁盘上
		String lxw_orc1_file = "/data/test_orc_file.orc";
		Writer writer = OrcFile.createWriter(new Path(lxw_orc1_file),
				OrcFile.writerOptions(new Configuration())
		          .setSchema(schema)
		          .stripeSize(67108864)
		          .bufferSize(131072)
		          .blockSize(134217728)
		          .compress(CompressionKind.ZLIB)
		          .version(OrcFile.Version.V_0_12));
		//要写入的内容
		String[] contents = new String[]{"1,a,aa","2,b,bb","3,c,cc","4,d,dd"};
		
		VectorizedRowBatch batch = schema.createRowBatch();
		for(String content : contents) {
			int rowCount = batch.size++;
			String[] logs = content.split(",", -1);
			for(int i=0; i<logs.length; i++) {
				((BytesColumnVector) batch.cols[i]).setVal(rowCount, logs[i].getBytes());
				//batch full
				if (batch.size == batch.getMaxSize()) {
				    writer.addRowBatch(batch);
				    batch.reset();
				}
			}
		}
		writer.addRowBatch(batch);
		writer.close();
	}
	
	public static void dataType() throws Exception {
		TypeDescription schema = TypeDescription.createStruct()
				.addField("field1", TypeDescription.createLong())
		        .addField("field2", TypeDescription.createDouble())
		        .addField("field3", TypeDescription.createBoolean())
		        .addField("field4", TypeDescription.createTimestamp())
		        .addField("field5", TypeDescription.createString());
		//输出ORC文件到HDFS
		String fileName = "/tmp/xuwl/test_orc_file_datatype.orc";
		Path path = new Path(fileName);
		FileSystem fs;
		try {
			fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
		    }
		} catch (Exception e) {
			e.printStackTrace();
			throw new KettleFileException(e.getCause());
		}
		Writer writer = OrcFile.createWriter(path,
				OrcFile.writerOptions(conf)
		          .setSchema(schema)
		          .stripeSize(67108864)
		          .bufferSize(131072)
		          .blockSize(134217728)
		          .compress(CompressionKind.ZLIB)
		          .version(OrcFile.Version.V_0_12));
		//要写入的内容
		Object[][] contents = new Object[][]{
				{1l,1.1,false,"2016-10-21 14:56:25","abcd"},
				{2l,1.2,true,"2016-10-22 14:56:25","中文"}
				};
		
		VectorizedRowBatch batch = schema.createRowBatch();
		for(Object[] content : contents) {
			int rowCount = batch.size++;
			((LongColumnVector) batch.cols[0]).vector[rowCount] = (long) content[0];
			((DoubleColumnVector) batch.cols[1]).vector[rowCount] =(double) content[1];
			((LongColumnVector) batch.cols[2]).vector[rowCount] =content[2].equals(true)?1:0;
			((TimestampColumnVector) batch.cols[3]).time[rowCount] 
					= (Timestamp.valueOf((String) content[3])).getTime();
			((BytesColumnVector) batch.cols[4]).setVal(rowCount, content[4].toString().getBytes("UTF8"));
			//batch full
			if (batch.size == batch.getMaxSize()) {
			    writer.addRowBatch(batch);
			    batch.reset();
			}
		}
		if(batch.size>0){
			writer.addRowBatch(batch);
		}
		writer.close();
	}
	
	public static void test() throws Exception{
		TypeDescription schema = TypeDescription.createStruct()
				.addField("id", TypeDescription.createLong())
		        .addField("article_title", TypeDescription.createString())
		        .addField("author", TypeDescription.createString())
		        .addField("pub_time", TypeDescription.createString())
		        .addField("article_type", TypeDescription.createString());
		//输出ORC文件到HDFS
		String fileName = "/user/test/orc/test_orc_file_article2.orc";
		Path path = new Path(fileName);
		FileSystem fs;
		try {
			fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
		    }
		} catch (Exception e) {
			e.printStackTrace();
			throw new KettleFileException(e.getCause());
		}
		
		OrcProxy writeProxy = new OrcProxy(conf, fileName, schema, true);
		writeProxy.start();
		
		//要写入的内容
		String driverName = "com.mysql.jdbc.Driver";
    	String url = "jdbc:mysql://localhost:3306/wuji";
    	String username = "root";
    	String password = "root";
    	long start = new Date().getTime();
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(url, username, password);
			Statement stmt = con.createStatement();
			String sql = "SELECT * FROM article limit 10";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				Object[] row = new Object[5];
				row[0] = rs.getLong("id");
				row[1] = rs.getString("article_title");
				row[2] = rs.getString("author");
				row[3] = rs.getString("pub_time");
				row[4] = rs.getString("article_type");
				writeProxy.writeRow(row);
			}
			
			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		writeProxy.close();
		long end = new Date().getTime();
		System.out.println("use:"+(end-start)/1000);
		
	}

}
