package com.chinacloud.esoutput;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.google.common.base.Strings;

public class EsTransferDao {
	/**
	 * 
	 * 初始化查询总条数对象,用游标方式
	 * 
	 * @param page
	 * @return
	 */
	public SearchResponse findCount(Client client, String index, String mainType, int size, String columnName,
			Date fromDate, Date toDate, String... findColumnNames) {
		SearchRequestBuilder srb = client.prepareSearch(index);
		srb.setTypes(mainType);
		srb.setSearchType(SearchType.QUERY_AND_FETCH);
		if (findColumnNames != null && findColumnNames.length > 0) {
			srb.addFields(findColumnNames);
		}
		srb.setSize(size);
		srb.setPreference("_primary_first");
		srb.setScroll(TimeValue.timeValueMinutes(1));
		if (!Strings.isNullOrEmpty(columnName) && toDate != null) {
			BoolQueryBuilder booQuery = QueryBuilders.boolQuery();
			RangeQueryBuilder range = QueryBuilders.rangeQuery(columnName).from(fromDate).to(toDate);
			booQuery.must(range);
		}
		SearchResponse scrollResponse = srb.execute().actionGet();
		return scrollResponse;
	}

	/**
	 * 一对多 此方法是更新索引，addColumnName为新增或更新的列，addDatas为列对应的值对象数组
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param addColumnName
	 *            添加的列名
	 * @param allMap
	 *            一个id对应多行数据，key为Id,value为多数据
	 */
	public BulkResponse upsertIndexManay(Client client, String index, String type, String addColumnName,
			Map<String, List<Map<String, Object>>> allMap) {
		BulkResponse bulkResponse = null;
		try {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			Set<String> ids = allMap.keySet();
			// System.out.println("ids=="+ids);
			for (String id : ids) {
				List<Map<String, Object>> addDatas = allMap.get(id);
				UpdateRequestBuilder updateRequestBuilder = initAddColumnBuilderManay(client, index, type, id,
						addColumnName, addDatas);
				bulkRequestBuilder.add(updateRequestBuilder);
			}
			bulkResponse = commitRequest(client, bulkRequestBuilder);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bulkResponse;
	}

	/**
	 * 一对多，被添加列里面有多条数据
	 * 此方法，是构更新UpdateRequestBuilder对象，如果表没addColumnName列，就新增，有就更新，addData为addColumnName对应的值
	 * 重要说明：此方法是一对多，addData里面只存了一个对象
	 * 
	 * @param index
	 *            索此名
	 * @param type
	 *            表名
	 * @param id
	 *            id
	 * @param addColumnName
	 *            新增列名
	 * @param addData
	 *            数据
	 * @return UpdateRequestBuilder对象
	 */
	private UpdateRequestBuilder initAddColumnBuilderManay(Client client, String index, String type, String id,
			String addColumnName, List<Map<String, Object>> addDatas) {
		if (!Strings.isNullOrEmpty(index) && !Strings.isNullOrEmpty(type) && !Strings.isNullOrEmpty(id)) {
			UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(index, type, id);
			try {
				XContentBuilder builder = jsonBuilder().startObject();
				builder.startArray(addColumnName);
				for (Map<String, Object> addData : addDatas) {
					builder.startObject();
					Set<String> keys = addData.keySet();
					for (String key : keys) {
						Object value = addData.get(key);
						builder.field(key, value);
					}
					builder.endObject();
				}
				builder.endArray();
				builder.endObject();
				updateRequestBuilder.setUpsert(builder).setDoc(builder);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return updateRequestBuilder;
		} else {
			return null;
		}
	}

	/**
	 * 提交请求
	 * 
	 * @param client
	 * @param bulkRequestBuilder
	 * @return
	 */
	public BulkResponse commitRequest(Client client, BulkRequestBuilder bulkRequestBuilder) {
		BulkResponse bulkResponse = null;
		try {
			bulkResponse = bulkRequestBuilder.execute().get();
			if (bulkResponse.hasFailures()) {
				System.out.println("提交出现错误......");
				System.err.println(bulkResponse.buildFailureMessage());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bulkResponse;
	}

}
