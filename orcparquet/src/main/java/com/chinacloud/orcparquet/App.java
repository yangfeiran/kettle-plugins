package com.chinacloud.orcparquet;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import com.chinacloud.orcparquet.util.ExampleParquetWriter;

/**
 * Hello world!
 *
 */
public class App {
	static String ClusterName = "nsstargate";
	private static final String HADOOP_URL = "hdfs://"+ClusterName;
	public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        conf.set("dfs.ha.namenodes."+ClusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn1", "172.16.50.24:8020");
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn2", "172.16.50.21:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider."+ClusterName, 
        		"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }
    
    public static final MessageType FILE_SCHEMA = Types.buildMessage()
		      .required(INT32).named("id")
		      .required(BINARY).as(UTF8).named("article_title")
		      .required(BINARY).as(UTF8).named("author")
		      .required(BINARY).as(UTF8).named("pub_time")
		      .required(BINARY).as(UTF8).named("article_type")
		      .named("test");
	
    public static void main( String[] args ) throws Exception
    {
    	String file = "/user/test/test_parquet_file_article2.parquet";
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
	    if (fs.exists(path)) {
	      fs.delete(path, true);
	    }
	    SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);
		ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
				.withConf(conf)
		        .withType(FILE_SCHEMA)
		        .build();
    	
    	
    	String driverName = "com.mysql.jdbc.Driver";
    	String url = "jdbc:mysql://localhost:3306/wuji";
    	String username = "root";
    	String password = "root";
    	long start = new Date().getTime();
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(url, username, password);
			Statement stmt = con.createStatement();
			
			//获取主键
			DatabaseMetaData dbmd= con.getMetaData();
			ResultSet rs0 = dbmd.getPrimaryKeys(null,null,"article");
			while (rs0.next()) {
				String keyname = rs0.getString( "PK_NAME" );
		        String col_name = rs0.getString( "COLUMN_NAME" );
				System.out.println(keyname+"=="+col_name);
			}
			rs0.close();
			
			/*String sql = "SELECT * FROM article limit 10";
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				Group group = f.newGroup();
				group.add("id", rs.getInt("id"));
				group.add("article_title", rs.getString("article_title"));
				group.add("author", rs.getString("author"));
				group.add("article_type", rs.getString("article_type"));
				group.add("pub_time", rs.getString("pub_time"));
				writer.write(group);
			}*/
			writer.close();
			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = new Date().getTime();
		System.out.println("use:"+(end-start)/1000);
    }
}
