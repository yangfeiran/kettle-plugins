package com.chinacloud.orcparquet.util;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ParquetFileUtil {
	
	static String ClusterName = "nsstargate";
	private static final String HADOOP_URL = "hdfs://"+ClusterName;
	public static Configuration conf;

    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn2", "172.16.50.80:8020");
        conf.set("dfs.namenode.rpc-address."+ClusterName+".nn1", "172.16.50.81:8020");
        conf.set("dfs.ha.namenodes."+ClusterName, "nn1,nn2");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider."+ClusterName, 
        		"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }
    
    public static final MessageType FILE_SCHEMA = Types.buildMessage()
		      .required(INT32).named("id")
		      .required(PrimitiveTypeName.BOOLEAN).named("bl")
		      .required(BINARY).as(UTF8).named("name")
		      .named("test");
    
	private static final String[] PATH1 = { "id"};
	private static final ColumnDescriptor C1 = FILE_SCHEMA.getColumnDescription(PATH1);
	private static final String[] PATH2 = { "name"};
	private static final ColumnDescriptor C2 = FILE_SCHEMA.getColumnDescription(PATH2);

	private static byte[] BYTES1 = "1".getBytes();
	private static byte[] BYTES2 = "testg".getBytes();
	private static final CompressionCodecName CODEC = CompressionCodecName.UNCOMPRESSED;

	private static final BinaryStatistics STATS1 = new BinaryStatistics();
	private static final BinaryStatistics STATS2 = new BinaryStatistics();

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "xuwl");
		//createFile2();
		crdateFolder();
	}
	
	/**
	 * @throws IOException 
	 * 
	 */
	private static void crdateFolder() throws IOException {
		Path path = new Path("/tmp/xuwl1");
		FileSystem fs = path.getFileSystem(conf);
	    if (!fs.exists(path)) {
	      fs.mkdirs(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
	    }
	    fs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
	}

	public static void createFile1() throws Exception {
		String file = "/tmp/xuwl1/test_parquet_file0.parquet";
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
	    if (fs.exists(path)) {
	      fs.delete(path, true);
	    }
		GroupWriteSupport.setSchema(FILE_SCHEMA, conf);
	    SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);
		ParquetWriter<Group> writer = new ParquetWriter<Group>(path, new GroupWriteSupport(),
				CODEC, 1024, 1024, 512, true, false, WriterVersion.PARQUET_2_0, conf);
		for (int i = 0; i < 100; i++) {
          writer.write(
              f.newGroup()
              .append("id", i)
              .append("name", UUID.randomUUID().toString()));
        }
        writer.close();
	}
	
	public static void createFile2() throws Exception {
		String file = "/tmp/xuwl1/test_parquet_type1.parquet";
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
	    if (fs.exists(path)) {
	      //fs.delete(path, true);
	    }
	    SimpleGroupFactory f = new SimpleGroupFactory(FILE_SCHEMA);
		Mode mode = Mode.OVERWRITE;
		int rowGroupSize = 134217728;//块大小:128M
		ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
				.withConf(conf)
		        .withType(FILE_SCHEMA)
		        .withWriteMode(mode)
		        .withRowGroupSize(rowGroupSize)
		        .build();
		for (int i = 0; i < 1000; i++) {
			Group group = f.newGroup();
			group.add("id", i);
			group.add("bl", i%3==0);
			group.add("name", UUID.randomUUID().toString());
			writer.write(group);
        }
        writer.close();
        fs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
	}
	
	/**
	 * 未成功
	 * @throws Exception
	 */
	public static void createFile3() throws Exception {
		String file = "/user/test/test_parquet_file3.parquet";
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(conf);
	    if (fs.exists(path)) {
	      fs.delete(path, true);
	    }
	    ParquetFileWriter w = new ParquetFileWriter(conf, FILE_SCHEMA, path);
	    w.start();
	    w.startBlock(2);
	    w.startColumn(C1, 1, CODEC);
	    w.writeDataPage(1, BYTES1.length, BytesInput.from(BYTES1), STATS1, RLE, RLE, PLAIN);
	    w.endColumn();
	    w.startColumn(C2, 1, CODEC);
	    w.writeDataPage(1, BYTES2.length, BytesInput.from(BYTES2), STATS2, RLE, RLE, PLAIN);
	    w.endColumn();
	    w.endBlock();
	    w.end(new HashMap<String, String>());
	}

}
