package com.chinacloud.esoutput;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.nutz.json.Json;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ESAPI {
	
	static Settings settings = Settings.settingsBuilder()
			.put("cluster.name", "elasticsearch").build();

	public static void main(String[] args) {
		try {
			//use es1.7.3
			/*Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", "elasticsearch").build();
			Client client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress(
							InetAddress.getByName("172.16.50.21"), 9300));*/
			
			//es2.x client api
			
			TransportClient client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(
							InetAddress.getByName("172.16.50.21"), 19300));
			
			/*GetRequestBuilder searchReq = client.prepareGet("jwzh", "ryxx", "1001");
			GetResponse res = searchReq.get();
			System.out.println(res.getSourceAsString());
			Object x = res.getSource().get("fwgjxx");
			System.out.println(x);
			Object y = res.getSource().get("fwgjxx_num");
			System.out.println(y);*/
			
			
			Set<String> ids = Sets.newHashSet();
			/*ids.add("1001");
			ids.add("1002");
			ids.add("1003");*/
			/*try {
				Class.forName("com.mysql.jdbc.Driver");
				String url = "jdbc:mysql://localhost/lagou";
			    Connection con = DriverManager.getConnection(url, "root", "root");
			    Statement st = con.createStatement();
			    ResultSet rs = st.executeQuery("SELECT distinct companyId from position limit 3000");
			    ResultSetMetaData metaData = rs.getMetaData();
				int n = metaData.getColumnCount();
				String parentId=null;
				int batch = 2000;
			    while (rs.next()) {
			    	ids.add(rs.getString(1));
			    }
			}catch (Exception e) {
				;
			}
			
			long start = System.currentTimeMillis();
			MultiGetResponse mGetResponse = client.prepareMultiGet()
					.add("lagou", "company", ids)
					.execute().actionGet();
			MultiGetItemResponse[] responses = mGetResponse.getResponses();*/
			/*for(int i=0;i<responses.length;i++){
				System.out.println(responses[i].getResponse().getId()+"=="
						+responses[i].getResponse().getSource());
			}*/
			
			/*TransportClient client2 = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(
							InetAddress.getByName("172.16.50.21"), 9300));
			List<Map<String, Object>> queryByIds = queryByIds(client2, ids);
			for (Map<String, Object> map : queryByIds) {
				System.out.println(map);
			}
			System.out.println(System.currentTimeMillis()-start);*/
			
			//getJobs();
			
			/*SearchRequestBuilder srb = client.prepareSearch("jwzh");
			srb.setTypes("ryxx");
			srb.setSearchType(SearchType.QUERY_AND_FETCH);
			srb.addFields("fwgjxx");
			//srb.addFields("fwgjxx_num");
			int size = 1000;
			srb.setSize(size );
			//srb.setPreference("_primary_first");
			srb.setScroll(TimeValue.timeValueMinutes(1));
			SearchResponse response = srb.execute().actionGet();
			for(SearchHit hit : response.getHits().getHits()){
				System.out.println(hit.getSource());
			}*/
			
			SearchRequestBuilder searchReq = client.prepareSearch("lagou");
			SearchRequestBuilder req = searchReq.setTypes("company");
			QueryBuilder qb = QueryBuilders.termQuery("jobs.education", "本科");
			req.setQuery(qb);
			req.setFrom(5).setSize(3);
			//req.addSort(SortBuilders.fieldSort("title").order(SortOrder.DESC));
			SearchResponse res = req.execute().actionGet();
			int k=0;
			for (SearchHit hit :res.getHits().getHits()) {
				//System.out.println("========"+(k++));
				System.out.println(hit.getSourceAsString());
			}
			
			//query(client);
			
			//index(client);
			
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<String, List<Map<String,Object>>> getJobs() {
		long s = System.currentTimeMillis();
		String[] ips = {"172.16.50.21","172.16.50.24"};
		int[] ports = {9300,9300};
		SQL4ESUtil sql4esUtil = new SQL4ESUtil("elasticsearch", ips, ports);
		List<Map<String,Object>> childList = new ArrayList<Map<String, Object>>();
		Map<String, List<Map<String,Object>>> childMap = Maps.newHashMap();
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost/lagou";
		    Connection con = DriverManager.getConnection(url, "root", "root");
		    Statement st = con.createStatement();
		    ResultSet rs = st.executeQuery("SELECT * from position order by companyId");
		    ResultSetMetaData metaData = rs.getMetaData();
			int n = metaData.getColumnCount();
			String parentId=null;
			int batch = 10000;
		    while (rs.next()) {
		    	if(parentId==null){
		    		parentId = rs.getString("companyId");
		    	}
		    	if(!parentId.equals(rs.getString("companyId"))){
		    		childMap.put(parentId, childList);
		    		childList = Lists.newArrayList();
		    		parentId = rs.getString("companyId");
		    	}
		    	if(childMap.size() == batch){
		    		System.out.println("调用一次。。");
		    		sql4esUtil.bulkUpdate("lagou", "company", "jobs", "jobs_num", "positionId > 0", childMap);
		    		childMap.clear();
		    	}
		    	Map<String,Object> map = Maps.newHashMap();
		    	for(int i=1;i<=n;i++){
		    		map.put(metaData.getColumnName(i), rs.getObject(i));
		    	}
		    	map.put("parentId", rs.getObject("companyId"));
		    	map.remove("companyId");
		    	map.remove("formatCreateTime");
		    	map.put("hashcode", "hash="+map.hashCode());
		    	childList.add(map);
			}
		    if(childMap.size()>0){
			    System.out.println("调用一次。。");
	    		sql4esUtil.bulkUpdate("lagou", "company", "jobs", "jobs_num", "positionId > 0", childMap);
	    		childMap.clear();
		    }
		} catch (Exception e) {
			e.printStackTrace();
		}
		sql4esUtil.close();
		System.out.println("all use:"+(System.currentTimeMillis()-s)/1000);
		return null;
	}
	
	public static List<Map<String, Object>> queryByIds(Client client, Set<String> ids) {
		SearchResponse response = client.prepareSearch("jwzh")
		        .setTypes("ryxx")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.queryStringQuery(converString(ids)).defaultField("_id"))
		        .execute()
		        .actionGet();
		List<Map<String, Object>> list = Lists.newArrayList();
		for(SearchHit hit : response.getHits().getHits()){
			list.add(hit.getSource());
		}
		return list;
	}
	
	public static String converString(Set<String> set) {
		StringBuffer sb = new StringBuffer();
		int i = 0;
		int len = set.size();
		//sb.append("\"");
		for (String str : set) {
			if (len == 1) {
				sb.append("\"");
				sb.append(str);
				sb.append("\"");
			} else if (len > 1) {
				if (i == 0) {
					sb.append("\"");
					sb.append(str);
					sb.append("\"");
					
				} else {
					sb.append(" OR \"");
					sb.append(str);
					sb.append("\"");
				}
			}
			i++;
		}
		//sb.append("\"");
		return sb.toString();
	}


	private static void query(Client client) {
		SearchRequestBuilder searchReq = client.prepareSearch("test");
		SearchRequestBuilder req = searchReq.setTypes("document");
		QueryBuilder qb = QueryBuilders.hasChildQuery(
			    "pages",                     
			    QueryBuilders.termQuery("text","revolution") 
		);
		req.setQuery(qb);
		//req.setFrom(6).setSize(3);
		//req.addSort(SortBuilders.fieldSort("title").order(SortOrder.DESC));
		SearchResponse res = req.execute().actionGet();
		int k=0;
		for (SearchHit hit :res.getHits().getHits()) {
			System.out.println("========"+(k++));
			System.out.println(hit.getId());
		}
		
	}

	private static void index(Client client) {
		IndexResponse response = null;
		String jsonStr = "";
		Map data = Json.fromJsonAsMap(Object.class, jsonStr);
		System.out.println(data);
		String id = String.valueOf(data.get("_id"));
		data.remove("_id");
		response = client.prepareIndex("docdive", "document", id)
				.setSource(JSON.toJSONString(data)).setRefresh(true).get();
		boolean status = response.isCreated();
		System.out.println(status);
	}
	

}
