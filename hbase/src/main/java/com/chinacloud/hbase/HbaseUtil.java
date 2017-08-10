package com.chinacloud.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;

/**
 * 
 * @author nivalsoul kskscr@163.com
 * @date 2017年2月27日 上午9:24:30
 *
 */
public class HbaseUtil {

	private Configuration conf;
	private Connection conn = null;

	/**
	 * 
	 */
	public HbaseUtil(String HBaseRootdir, String HBaseMaster, String zkHosts, String zkPort) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", HBaseRootdir);
		conf.set("hbase.master", HBaseMaster);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		conf.set("hbase.zookeeper.quorum", zkHosts);
		
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		String[] columnFamilies = { "company" };
		HbaseUtil hbaseUtil = new HbaseUtil("hdfs://master/hbase", 
				"172.16.50.22:60000", "172.16.50.22", "2181");
		
		columnFamilies = new String[]{"company","jobs"};
		//hbaseUtil.addFamilys("abc", columnFamilies);
		
		//hbaseUtil.createTable("lagou", columnFamilies);
		String[] columns = {"name","age"};
		Object[] values = {"李四",35};
		//hbaseUtil.addData("abc", "key002", "person", columns, values);
		hbaseUtil.deleteTable("lagou2");
		
		//hbaseUtil.getResult("lagou", "108");
		//hbaseUtil.scanTable("lagou");
	}

	/**
	 * 创建数据库表
	 * @return 
	 */

	public boolean createTable(String tableName, String[] columnFamilies){
		boolean result = true;
		try {
			// 创建一个数据库管理员
			HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
			if (hAdmin.tableExists(tableName)) {
				System.out.println("表" + tableName + "已经存在，尝试添加列族");
				addFamilys(hAdmin, tableName, columnFamilies);
			} else {
				// 新建一个表描述
				HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
				// 在表描述里添加列族
				for (String columnFamily : columnFamilies) {
					HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
					hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);// 设置压缩类型
					tableDesc.addFamily(hColumnDescriptor);
				}
				// 根据配置好的表描述建表
				hAdmin.createTable(tableDesc);
				System.out.println("创建" + tableName + "表成功");
			}
			hAdmin.close();
		} catch (IOException e) {
			result = false;
			e.printStackTrace();
		}
		return result;
	}
	
	public boolean addFamilys(HBaseAdmin hAdmin, 
			String tableName, String[] columnFamilies) throws IOException {
		boolean result = true;
		// 创建一个数据库管理员
		if(hAdmin == null)
			hAdmin = (HBaseAdmin) conn.getAdmin();
		try {
			if (hAdmin.tableExists(tableName)) {
				HColumnDescriptor[] families = hAdmin
						.getTableDescriptor(TableName.valueOf(tableName)).getColumnFamilies();
				List<String> familyNames = Lists.newArrayList();
		        for (int i = 0; i < columnFamilies.length; i++) {
		            familyNames.add(families[i].getNameAsString());
		        }
				for (String columnFamily : columnFamilies) {
					if(!familyNames.contains(columnFamily)){
						HColumnDescriptor column = new HColumnDescriptor(columnFamily);
						column.setCompressionType(Compression.Algorithm.SNAPPY);// 设置压缩类型
						hAdmin.addColumn(tableName, column);
					}
				}
			}
			hAdmin.close();
		} catch (IOException e) {
			result = false;
			e.printStackTrace();
		}
		return result;
	}
	
	/**
     * 插入或者更新一行数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param family 列簇
     * @param columns 限定符（列键名）
     * @param values 列键值
     * @return true: 插入成功; false: 插入失败
     * @throws IOException
     */
    public boolean addData(String tableName, String rowKey, String family, 
    		String[] columns, Object[] values) throws IOException{
        try {
            HTable hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));//参数是 行键

            for(int i = 0;i<columns.length; i++){
	            put.addColumn(
	                    Bytes.toBytes(family),
	                    Bytes.toBytes(columns[i]),
	                    Bytes.toBytes(String.valueOf(values[i]))
	            );
            }

            hTable.put(put);
            hTable.close();
            
            return true;
        } catch (IOException e){
            e.printStackTrace();
        }
        return  false;
    }
    
    /**
     * 添加多行数据
     * @param tableName
     * @param family
     * @param rows 数据行：rowkey->Map<String, Object>> data
     * @return
     * @throws IOException
     */
    public boolean addData(String tableName, String family, 
    		Map<String, Map<String, Object>> rows){
        try {
            HTable hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
            hTable.setAutoFlush(false, true);
            hTable.setWriteBufferSize(10 * 1024 * 1024);//10M
            Iterator<Entry<String, Map<String, Object>>> iter = rows.entrySet().iterator();
            List<Put> puts = Lists.newArrayList();
			while(iter.hasNext()){
				Entry<String, Map<String, Object>> row = iter.next();
				String rowKey = row.getKey();
				Put put = new Put(Bytes.toBytes(rowKey));
				
				Map<String, Object> data = row.getValue();
				Iterator<Entry<String, Object>> r = data.entrySet().iterator();
				while(r.hasNext()){
					Entry<String, Object> entry = r.next();
					put.addColumn(
		                    Bytes.toBytes(family),
		                    Bytes.toBytes(entry.getKey()),
		                    Bytes.toBytes(String.valueOf(entry.getValue()))
		            );
				}
	            puts.add(put);
			}
			hTable.put(puts);
            hTable.close();
            
            return true;
        } catch (IOException e){
            e.printStackTrace();
        }
        return  false;
    }
    
    /* get data. */
    public void getResult(String tableName, String rowKey)
            throws IOException {
        /* get table. */
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("------------------------------------");
            System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest: " + cell.getTimestamp());
        }
    }

    /* scan table. */
    public void getResultScan(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("------------------------------------");
                    System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
                    System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                    System.out.println("timest: " + cell.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }
    /**
     * 全表扫描
     */
    public void scanTable(String tableName) throws IOException {
        // 建立一个数据库的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        // 创建一个扫描对象
        Scan scan = new Scan();
        // 扫描全表输出结果
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                                "时间戳:" + cell.getTimestamp());
            }
        }
        // 关闭资源
        results.close();
        table.close();
        conn.close();
    }

    /* range scan table. */
    public void getResultScan(String tableName, String start_rowkey,
            String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (Cell cell : r.listCells()) {
                    System.out.println("------------------------------------");
                    System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
                    System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
                    System.out.println("timest: " + cell.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }

    /* get column data. */
    public void getResultByColumn(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        /* get table. */
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);

        for (Cell cell : result.listCells()) {
            System.out.println("------------------------------------");
            System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest: " + cell.getTimestamp());
        }
    }

    /* update. */
    public void updateTable(String tableName, String rowKey,
            String familyName, String columnName, String value)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /* get multi-version data. */
    public void getResultByVersion(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (Cell cell : result.listCells()) {
            System.out.println("------------------------------------");
            System.out.println("timest: " + cell.getSequenceId());
            System.out.println("rowkey: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("column: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value : " + new String(CellUtil.cloneValue(cell)));
            System.out.println("timest: " + cell.getTimestamp());
        }
    }

    /* delete column. */
    public void deleteColumn(String tableName, String rowKey,
            String falilyName, String columnName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    /* delete row. */
    public void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }

    /* delete table. */
    public void deleteTable(String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println(tableName + " is deleted!");
    }
	
	public void close() {
		if(conn!=null){
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
