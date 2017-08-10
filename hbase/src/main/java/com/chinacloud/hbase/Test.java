package com.chinacloud.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author nivalsoul kskscr@163.com
 * @date 2017年3月3日 下午1:29:38
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long s = System.currentTimeMillis();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost/lagou";
		    Connection con = DriverManager.getConnection(url, "root", "root");
		    Statement st = con.createStatement();
		    ResultSet rs = st.executeQuery("SELECT * from company");
		    ResultSetMetaData metaData = rs.getMetaData();
			int n = metaData.getColumnCount();
			HbaseUtil hbaseUtil = new HbaseUtil("hdfs://master/hbase", 
					"172.16.50.22:60000", "172.16.50.22", "2181");
			String[] columnFamilies = new String[]{"company","jobs"};
			String tableName = "lagou2";
			//hbaseUtil.createTable(tableName, columnFamilies);
			
			int batch = 10000;
			Map<String, Map<String,Object>> dataMap = Maps.newHashMap();
		    while (rs.next()) {
		    	if(dataMap.size() == batch){
		    		System.out.println("调用一次。。");
		    		hbaseUtil.addData(tableName, "company", dataMap);
		    		dataMap.clear();
		    	}
		    	Map<String,Object> map = Maps.newHashMap();
		    	for(int i=1;i<=n;i++){
		    		map.put(metaData.getColumnName(i), rs.getObject(i));
		    	}
		    	dataMap.put(rs.getString("companyId"), map);
		    	
			}
		    if(dataMap.size()>0){
			    System.out.println("调用一次。。");
			    hbaseUtil.addData(tableName, "company", dataMap);
	    		dataMap.clear();
		    }
		    hbaseUtil.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("all use:"+(System.currentTimeMillis()-s)/1000);
	}

}
