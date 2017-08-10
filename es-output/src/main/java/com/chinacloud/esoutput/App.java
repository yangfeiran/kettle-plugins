package com.chinacloud.esoutput;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception{
    	/*Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "elasticsearch-1.7.3").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress("172.16.50.81", 11300))
				.addTransportAddress(new InetSocketTransportAddress("172.16.50.80", 11300));
		SearchRequestBuilder req = client.prepareSearch("test2");
		QueryBuilder qb = QueryBuilders.matchAllQuery();
		SearchResponse res = req.setQuery(qb).setSize(1).execute().actionGet();
		System.out.println(res.getHits().totalHits());*/
    	
    	//execMysql();
	
    	
    	List<Map<String, Object>> list = Lists.newArrayList();
    	Map<String, Object> map = Maps.newHashMap();
    	map.put("parentId", 1);
    	map.put("name", "aa");
    	map.put("age", 14l);
    	map.put("sex", "男1");
    	map.put("_rowid", 1);
    	map.put("XT_ZHXGSJ", "2015");
    	list.add(map);
    	map = Maps.newHashMap();
    	map.put("parentId", 1);
    	map.put("name", "bb");
    	map.put("age", 22l);
    	map.put("sex", "女1");
    	map.put("_rowid", 2);
    	map.put("XT_ZHXGSJ", "2015");
    	list.add(map);
    	map = Maps.newHashMap();
    	map.put("parentId", 1);
    	map.put("name", "cc");
    	map.put("age", 25l);
    	map.put("sex", "女1");
    	map.put("_rowid", 1);
    	map.put("XT_ZHXGSJ", "2016");
    	list.add(map);
    	
    	List<Map<String, Object>> rows = Lists.newArrayList();
    	Map<String, Object> map2 = Maps.newHashMap();
    	map2.put("parentId", 1);
    	map2.put("name", "aa");
    	map2.put("age", 14l);
    	map2.put("sex", "男2");
    	map2.put("_rowid", 1);
    	map2.put("XT_ZHXGSJ", "2015");
    	rows.add(map2);
    	map2 = Maps.newHashMap();
    	map2.put("parentId", 1);
    	map2.put("name", "bb");
    	map2.put("age", 22l);
    	map2.put("sex", "女2");
    	map2.put("_rowid", 2);
    	map2.put("XT_ZHXGSJ", "2015");
    	rows.add(map2);
    	
    	List<Map<String, Object>> updateList = Lists.newArrayList();
		updateList.addAll(rows);
		for (int i = 0; i < list.size(); i++) {
			Map<String, Object> map1 = list.get(i);
			boolean exist = false;
			for (int j=0;j<updateList.size();j++) {
				Map<String, Object> data = updateList.get(j);
				Object rid = data.get("_rowid");
				if(rid==map1.get("_rowid") || rid.equals(map1.get("_rowid"))){//兼顾key不存在和null值
					String t1 = (String) map1.get("XT_ZHXGSJ");
					String t2 = (String) data.get("XT_ZHXGSJ");
					if(t1!=null && t1.compareTo(t2)>0){//获取旧的重复数据最新一条
						updateList.set(j, map1);
					}
					exist = true;
					break;
				}
			}
			if(!exist){
				updateList.add(map1);
			}
		}
		System.out.println(updateList);
		
		String json = "{\"logger\":{\"traceCapable\":true,\"name\":\"com.lagou.entity.position.PositionVo\"},\"companyId\":451,\"positionId\":2935806,\"jobNature\":\"全职\",\"financeStage\":\"上市公司\",\"companyName\":\"腾讯\",\"companyFullName\":\"腾讯科技(深圳)有限公司\",\"companySize\":\"2000人以上\",\"industryField\":\"移动互联网 ,游戏\",\"positionName\":\"OMG096-腾讯财经创投记者\",\"city\":\"北京\",\"createTime\":\"2017-04-07\",\"salary\":\"10k-20k\",\"workYear\":\"1-3年\",\"education\":\"本科\",\"positionAdvantage\":\"腾讯财经\",\"companyLabelList\":[\"免费班车\",\"成长空间\",\"年度旅游\",\"岗位晋升\"],\"userId\":7361062,\"companyLogo\":\"image1/M00/00/03/CgYXBlTUV_qALGv0AABEuOJDipU378.jpg\",\"haveDeliver\":false,\"score\":0,\"adWord\":0,\"adTimes\":0,\"adBeforeDetailPV\":0,\"adAfterDetailPV\":0,\"adBeforeReceivedCount\":0,\"adAfterReceivedCount\":0,\"isCalcScore\":false,\"searchScore\":0,\"district\":\"海淀区\"}";
		Set<String> row = execMysql().get(0).keySet();
		Set<String> keySet = JSON.parseObject(json).keySet();
		for (String key : row) {
			if(!keySet.contains(key)){
				System.out.println(key);
			}
		}
    }
    
    public static List<Map<String, Object>> execMysql() {
    	String driverName = "com.mysql.jdbc.Driver";
    	String url = "jdbc:mysql://localhost:3306/lagou";
    	String username = "root";
    	String password = "root";
    	List<Map<String, Object>> result = Lists.newArrayList();
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(url, username, password);
			Statement stmt = con.createStatement();
			String sql = "SELECT * FROM position limit 1";
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int nrCols = rsmd.getColumnCount();
			if (rs != null) {
				while (rs.next()) {
					Map<String, Object> map = Maps.newHashMap();
					for (int i = 1; i <= nrCols; i++) {
						map.put(rsmd.getColumnLabel(i), rs.getObject(i));
					}
					result.add(map);
				}
				rs.close();
			}
			stmt.close();
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
