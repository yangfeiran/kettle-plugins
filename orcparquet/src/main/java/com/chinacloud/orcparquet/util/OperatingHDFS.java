package com.chinacloud.orcparquet.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatingHDFS {
	private static final Logger logger = LoggerFactory.getLogger(OperatingHDFS.class);
	
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
		uploadToHdfs();
	}
	
	
	/**
     * 上传文件到HDFS上去
     */
    private static void uploadToHdfs() throws IOException {
        String localSrc = "E:\\test\\article01.txt";
        String dst = "/tmp/xuwl/article06.txt";
        FileSystem fs = FileSystem.get(URI.create(HADOOP_URL), conf);
        long start = new Date().getTime();

       /* InputStream in = new FileInputStream(localSrc);
        InputStreamReader isr = new InputStreamReader(in, "GBK");
        OutputStream out = fs.create(new Path(HADOOP_URL+dst), true);
        IOUtils.copy(isr, out, "UTF8");*/
        
        //该方法更快
		FSDataOutputStream outputStream=fs.create(new Path(dst));
		String fileContent = FileUtils.readFileToString(new File(localSrc), "GBK");
		outputStream.write(fileContent.getBytes());
        outputStream.close();
        
        long end = new Date().getTime();
        System.out.println("use:"+(end-start));
        
    }


    /**
     * 从HDFS上读取文件
     */
    private static void readFromHdfs() throws IOException {
        String dst = "qq-hdfs.txt";
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataInputStream hdfsInStream = fs.open(new Path(dst));

        OutputStream out = new FileOutputStream("f:/eeeeeeeeeeee.txt");
        byte[] ioBuffer = new byte[1024];
        int readLen = hdfsInStream.read(ioBuffer);

        while (-1 != readLen) {
            out.write(ioBuffer, 0, readLen);
            readLen = hdfsInStream.read(ioBuffer);
        }
        out.close();
        hdfsInStream.close();
        fs.close();
    }


    /**
     * 以append方式将内容添加到HDFS上文件的末尾;注意：文件更新，需要在hdfs-site.xml中添<property><name>dfs.append.support</name><value>true</value></property>
     */
    private static void appendToHdfs() throws IOException {
        String dst = "test.txt";
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FSDataOutputStream out = fs.append(new Path(dst));

        int readLen = "zhangzk add by hdfs java api".getBytes().length;

        while (-1 != readLen) {
            out.write("zhangzk add by hdfs java api".getBytes(), 0, readLen);
        }
        out.close();
        fs.close();
    }


    /**
     * 从HDFS上删除文件
     */
    private static void deleteFromHdfs() throws IOException {
        String dst = "user/news/";
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        fs.deleteOnExit(new Path(dst));
        fs.close();
    }


    /**
     * 遍历HDFS上的文件和目录
     */
    private static void getDirectoryFromHdfs() throws IOException {
        String dst = "user/zhangzk";
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        FileStatus fileList[] = fs.listStatus(new Path(dst));
        int size = fileList.length;
        for (int i = 0; i < size; i++) {
            System.out.println("name:" + fileList[i].getPath().getName() + "/t/tsize:" + fileList[i].getLen());
        }
        fs.close();
    }

}
