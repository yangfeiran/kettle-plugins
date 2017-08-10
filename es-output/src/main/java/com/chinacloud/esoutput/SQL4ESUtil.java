package com.chinacloud.esoutput;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.get.MultiGetRequest.Item;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.joda.time.LocalTime;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SQL4ESUtil {
	
	private static Logger logger = Logger.getLogger(SQL4ESUtil.class);
	
	private Client client = null;
	
	private String clusterName = "elasticsearch";
	private String[] ips;
	private int[] ports;
	
	/**
	 * 该实例会自动创建读取数据的连接，使用完毕请调用close()关闭连接！
	 * @param clusterName
	 * @param ip
	 * @param port
	 */
	public SQL4ESUtil(String clusterName, String ip, int port){
		if(clusterName!=null && !clusterName.equals(""))
		    this.clusterName = clusterName;
		this.ips = new String[]{ip};
		this.ports = new int[]{port};
		
		try {
			client = getClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 该实例会自动创建读取数据的连接，使用完毕请调用close()关闭连接！
	 * @param clusterName
	 * @param ips
	 * @param ports
	 */
	public SQL4ESUtil(String clusterName, String[] ips, int[] ports){
		if(clusterName!=null && !clusterName.equals(""))
		    this.clusterName = clusterName;
		this.ips = new String[ips.length];
		this.ports = new int[ports.length];
		for(int i=0;i<ips.length;i++){
			this.ips[i] = ips[i];
			this.ports[i] = ports[i];
		}
		
		try {
			client = getClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public Client getClient() throws UnknownHostException {
		//es2.x client api
		Settings settings = settings = Settings.settingsBuilder().put("cluster.name", clusterName)
				.put("node.name", "node"+System.currentTimeMillis())
				.put("client.transport.sniff", true).build();
		TransportClient client = TransportClient.builder().settings(settings).build();
		for(int i=0;i<ips.length;i++){
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ips[i]), ports[i]));
		}
		
		//es1.7 client api
		/*Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName).build();
		TransportClient client = new TransportClient(settings);
		for(int i=0;i<ips.length;i++){
			client.addTransportAddress(new InetSocketTransportAddress(ips[i], ports[i]));
		}*/
				
		return client;
	}
	
	public void open() {
		try {
			client = getClient();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		if(client!=null){
			client.close();
			client = null;
		}
	}
	
	public boolean disconnected() {
		return client == null;
	}
	
	/**
	 * 单次提交
	 * @param database 索引库名称
	 * @param table    表名
	 * @param data     一行数据，map类型，指定主键的话key为"_id"
	 * @return 
	 */
	public boolean singleRequest(String database, String table, Map<String, Object> data) {
		boolean status = false;
		try {
			IndexResponse response;
			String id = null;
			if(data.containsKey("_id")){
				id = String.valueOf(data.get("_id"));
				data.remove("_id");
			}
			IndexRequestBuilder req = client.prepareIndex(database, table, id);
			if(data.containsKey("_parent")){
				String parent = data.get("_parent").toString();
				data.remove("_parent");
				req = req.setParent(parent);
			}
			long v = -1;
			if(data.containsKey("_version")){
				v = Long.valueOf(data.get("_version").toString());
				data.remove("_version");
				req = req.setVersion(v);
			}
			//必须先setVersion再setSource，为何？
			req = req.setSource(JSON.toJSONString(data));
			response = req.setRefresh(true).get();
			status = response.isCreated();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	/**
	 * bulk批量提交
	 * @param database 索引库名称
	 * @param table    表名
	 * @param rows     多行数据，每行都是一个map类型，指定主键的话key为"_id"
	 * @return         成功返回OK，否则返回错误信息
	 */
	public String bulkRequest(String database, String table, List<Map<String, Object>> rows) {
		String result = "OK";
		try {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> data : rows) {
				String id = null;
				if(data.containsKey("_id")){
					id = String.valueOf(data.get("_id"));
					data.remove("_id");
				}
				IndexRequestBuilder req = client.prepareIndex(database, table, id);
				if(data.containsKey("_parent")){
					String parent = data.get("_parent").toString();
					data.remove("_parent");
					req = req.setParent(parent);
				}
				long v = -1;
				if(data.containsKey("_version")){
					v = Long.valueOf(data.get("_version").toString());
					data.remove("_version");
					req = req.setVersion(v);
				}
				//必须先setVersion再setSource，为何？
				req = req.setSource(JSON.toJSONString(data));
				bulkRequest.add(req);
			}
			BulkResponse bulkResponse = bulkRequest.setRefresh(true).get();
			if (bulkResponse.hasFailures()) {
			     result = bulkResponse.buildFailureMessage();
			}
		} catch (Exception e) {
			result = e.getMessage();
			e.printStackTrace();
		}
		return result;
	}
	
	public String bulkAdd(String database, String table, List<Map<String, Object>> rows) {
		String result = "OK";
		try {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> data : rows) {
				String id = UUID.randomUUID().toString();
				if(data.containsKey("_id")){
					id = String.valueOf(data.get("_id"));
					data.remove("_id");
				}
				UpdateRequestBuilder urb = client.prepareUpdate(database, table, id);
				if(data.containsKey("_parent")){
					String parent = data.get("_parent").toString();
					data.remove("_parent");
					urb = urb.setParent(parent);
				}
				urb.setRetryOnConflict(3)
				.setUpsert(data).setDoc(data);
				bulkRequest.add(urb);
			}
			BulkResponse bulkResponse = bulkRequest.execute().get();
			if (bulkResponse.hasFailures()) {//有错时重试
				/*Thread.sleep(3000);
				bulkResponse = bulkRequest.execute().get();
				if (bulkResponse.hasFailures()) {//有错时重试
					Thread.sleep(5000);
					bulkResponse = bulkRequest.execute().get();
					if (bulkResponse.hasFailures()){
						result = bulkResponse.buildFailureMessage();
					}
				}*/
				result = bulkResponse.buildFailureMessage();
			}
		} catch (Exception e) {
			result = e.getMessage();
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 根据ID查询一条数据
	 * @param database
	 * @param table
	 * @param id
	 * @return 包含了_id和_version的原始数据
	 */
	public Map<String, Object> getOneByID(String database, String table, String id) {
		Map<String, Object> data = null;
		try {
			GetResponse response = client.prepareGet(database, table, id).get();
			data = response.getSource();
			//data.put("_version", response.getVersion());
			//data.put("_id", id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return data;
	}
	
	public Map<String, Map<String, Object>> queryByIds(String database, String table, 
			Set<String> ids, String[] fields) {
		Map<String, Map<String, Object>> data = Maps.newHashMap();
		try {
			MultiGetRequestBuilder multiRequestBuilder = client.prepareMultiGet();
	    	for (String id : ids) {
	    		multiRequestBuilder.add(new Item(database, table, id)
	    				.fetchSourceContext(new FetchSourceContext(fields)));
	    	}
			MultiGetResponse mGetResponse = multiRequestBuilder.get();
			MultiGetItemResponse[] responses = mGetResponse.getResponses();
			for(int i=0;i<responses.length;i++){
				data.put(responses[i].getResponse().getId(),responses[i].getResponse().getSource());
			}
			
			//该方法结果数最大限制为1024
			/*SearchResponse response = client.prepareSearch(database)
			        .setTypes(table)
			        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			        .setQuery(QueryBuilders.queryStringQuery(converString(ids)).defaultField("_id"))
			        .execute()
			        .actionGet();
			for(SearchHit hit : response.getHits().getHits()){
				data.put(hit.getId(),hit.getSource());
			}*/
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	public String testServer(String index) {
		try {
			SearchRequestBuilder req = client.prepareSearch(index);
			QueryBuilder qb = QueryBuilders.matchAllQuery();
			SearchResponse res = req.setQuery(qb).setSize(1).execute().actionGet();
			return "OK";
		} catch (Exception e) {
			return e.getMessage();
		}
	}
	
	public boolean createIndex(String index) {
		try {
			CreateIndexResponse res = client.admin().indices().prepareCreate(index).get();
			return res.isAcknowledged();
		} catch (Exception e) {
			if(e instanceof IndexAlreadyExistsException)
				return true;
			return false;
		}
	}

	/**
	 * @param database
	 * @param table
	 * @param id
	 * @param parentData 父表数据
	 * @param field
	 * @param statCnd 
	 * @param statFieldName 
	 * @param olist
	 * @return
	 */
	private UpdateRequestBuilder bulkUpdate(String database, String table, String id, 
			Map<String, Object> parentData, String field, String statFieldName, 
			String statCnd, List<Map<String, Object>> rows) {
		try {
			if(parentData == null)
				parentData = Maps.newHashMap();
			List<Map<String, Object>> list = (List<Map<String, Object>>) parentData.get(field);
			if(list == null)
				list = Lists.newArrayList();
			Integer field_num = (Integer) parentData.get(statFieldName);
			if(field_num == null)
				field_num = 0;
			
			List<Map<String, Object>> updateList = Lists.newArrayList();
			updateList.addAll(rows);
			for (int i = 0; i < list.size(); i++) {
				Map<String, Object> map = list.get(i);
				boolean exist = false;
				for (int j=0; j < updateList.size(); j++) {
					Map<String, Object> data = updateList.get(j);
					Object rid = data.get("_rowid");
					if(rid == null){
						rid = data.get("ID");
					}
					if(rid==map.get("_rowid") || rid!=null && rid.equals(map.get("_rowid"))){//兼顾key不存在和null值
						exist = true;
						break;
					}
				}
				if(!exist){
					updateList.add(map);
				}
			}
			if(!Strings.isNullOrEmpty(statFieldName)){
				for (Map<String, Object> data : rows) {
					field_num += stat(data, statCnd);
				}
			}
			
			UpdateRequestBuilder urb = client.prepareUpdate(database, table, id);
			XContentBuilder builder = jsonBuilder().startObject();
			builder.field(field, updateList);
			if(!Strings.isNullOrEmpty(statFieldName))
				builder.field(statFieldName, field_num);
			builder.endObject();
			urb.setUpsert(builder).setDoc(builder);
			return urb;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * @param database
	 * @param table
	 * @param fieldName
	 * @param statFieldName
	 * @param statCnd
	 * @param map
	 * @return
	 */
	public String bulkUpdate(String database, String table, String fieldName, 
			String statFieldName, String statCnd, Map<String, List<Map<String, Object>>> map) {
		String result = "OK";
		Set<String> keys = map.keySet();
		System.out.println(LocalTime.now()+"==begin to query parent data, ids size:"+keys.size());
		long start = System.currentTimeMillis();
		String[] queryFields = {fieldName};
		Map<String, Map<String, Object>> data = queryByIds(database, table, keys, queryFields );
		System.out.println(LocalTime.now()+"==query parent data finish, use:"+(System.currentTimeMillis()-start)/1000);
		start = System.currentTimeMillis();
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (String id : keys) {
			int size = 0;
			if(data.get(id) != null){
				data.get(id).size();
			}
			if(map.get(id).size() > size){
				size = map.get(id).size();
			}
			if(size > 5000){
				result = "the child values of pId[" + id + "] for the field[" + fieldName + "] of "
						+ "table[" + table + "]\n is too large(limit 5000, now is " + size + ")!";
				System.out.println(LocalTime.now()+"***"+result);
				return result;
			}
			UpdateRequestBuilder urb = bulkUpdate(database, table, id, data.get(id),
					fieldName, statFieldName, statCnd, map.get(id));
			urb.setRetryOnConflict(3);
			bulkRequest.add(urb);
		}
		System.out.println(LocalTime.now()+"==prepare update data finish, use:"+(System.currentTimeMillis()-start)/1000);
		start = System.currentTimeMillis();
		BulkResponse bulkResponse;
		try {
			bulkResponse = bulkRequest.execute().get();
			if (bulkResponse.hasFailures()) {//有错时重试
				/*Thread.sleep(3000);
				bulkResponse = bulkRequest.execute().get();
				if (bulkResponse.hasFailures()) {//有错时重试
					Thread.sleep(5000);
					bulkResponse = bulkRequest.execute().get();
					if (bulkResponse.hasFailures()){
						result = bulkResponse.buildFailureMessage();
					}
				}*/
				result = bulkResponse.buildFailureMessage();
			}
		} catch (Exception e) {
			result = e.getMessage();
			e.printStackTrace();
		}

		System.out.println(LocalTime.now()+"==finish update..");
		System.out.println("use:"+(System.currentTimeMillis()-start)/1000);

		return result;
	}

	/**
	 * @param data
	 * @param statCnd，比较条件为：fieldName opt value，opt为比较符，支持：=,!=,<,>
	 * 如果为“<”或“>”，则比较字段值必须是数字！
	 * @return
	 */
	private int stat(Map<String, Object> data, String statCnd) {
		int num = 0;
		try {
			String[] cnd = statCnd.split(" "); 
			switch (cnd[1]) {
			case "=":
				if(cnd[2].equals(data.get(cnd[0])))
					num = 1;
				break;
			case "!=":
				if(!cnd[2].equals(data.get(cnd[0])))
					num = 1;
				break;
			case "<":
				if(Double.parseDouble(data.get(cnd[0]).toString())<Double.parseDouble(cnd[2]))
					num = 1;
				break;
			case ">":
				if(Double.parseDouble(data.get(cnd[0]).toString())>Double.parseDouble(cnd[2]))
					num = 1;
				break;

			default:
				break;
			}
		} catch (Exception e) {
			;
		}
		
		return num;
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

	
}
