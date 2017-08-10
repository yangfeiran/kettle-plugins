package com.chinacloud.esoutput;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.nutz.dao.Dao;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import com.google.common.base.Strings;

@Step(
	id = "ESOutputPlugin", 
	image = "esoutput.png", 
	i18nPackageName = "com.chinacloud.esoutput", 
	name = "Plugin.Name", 
	description = "Plugin.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class EDBOutMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = EDBOutMeta.class;
	
	/*****命名参数和环境变量*****/
	private String jobId;
	private String checkColumn;
	private String end;
	private String useLastValue;
	private String driverName;
	private String mysqlUrl;
	private String username;
	private String password;
	private String table;
	private String updateField;
	private String dateFields;
	
	/*****界面变量*****/
	private String outputType;
	private String url;
	private String clusterName;
	private String database;
	private String outputTable;
	private String fieldName;
	private String statFieldName;
	private String statCnd;
	private String batchSize;	

	public void setDefault() {
		this.outputType = "add-only";
		this.url = "192.168.1.111:9300";
		this.clusterName = "elasticsearch";
		this.database = "myindex";
		this.outputTable = "mytype";
		this.fieldName = "myfield";
		this.statFieldName = "myfield_num";
		this.statCnd = "checkid = 0";
		this.batchSize = "5000";
	}

	public String getXML() throws KettleValueException {
		StringBuilder retval = new StringBuilder();
		retval.append("    ").append(XMLHandler.addTagValue("outputType", this.outputType));
		retval.append("    ").append(XMLHandler.addTagValue("url", this.url));
		retval.append("    ").append(XMLHandler.addTagValue("clusterName", this.clusterName));
		retval.append("    ").append(XMLHandler.addTagValue("database", this.database));
		retval.append("    ").append(XMLHandler.addTagValue("outputTable", this.outputTable));
		retval.append("    ").append(XMLHandler.addTagValue("fieldName", this.fieldName));
		retval.append("    ").append(XMLHandler.addTagValue("statFieldName", this.statFieldName));
		retval.append("    ").append(XMLHandler.addTagValue("statCnd", this.statCnd));
		retval.append("    ").append(XMLHandler.addTagValue("batchSize", this.batchSize));
		return retval.toString();
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {
		this.outputType = XMLHandler.getTagValue(stepnode, "outputType");
		this.url = XMLHandler.getTagValue(stepnode, "url");
		this.clusterName = XMLHandler.getTagValue(stepnode, "clusterName");
		this.database = XMLHandler.getTagValue(stepnode, "database");
		this.outputTable = XMLHandler.getTagValue(stepnode, "outputTable");
		this.fieldName = XMLHandler.getTagValue(stepnode, "fieldName");
		this.statFieldName = XMLHandler.getTagValue(stepnode, "statFieldName");
		this.statCnd = XMLHandler.getTagValue(stepnode, "statCnd");
		this.batchSize = XMLHandler.getTagValue(stepnode, "batchSize");
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "outputType", this.outputType);
		rep.saveStepAttribute(id_transformation, id_step, "url", this.url);
		rep.saveStepAttribute(id_transformation, id_step, "clusterName", this.clusterName);
		rep.saveStepAttribute(id_transformation, id_step, "database", this.database);
		rep.saveStepAttribute(id_transformation, id_step, "outputTable", this.outputTable);
		rep.saveStepAttribute(id_transformation, id_step, "fieldName", this.fieldName);
		rep.saveStepAttribute(id_transformation, id_step, "statFieldName", this.statFieldName);
		rep.saveStepAttribute(id_transformation, id_step, "statCnd", this.statCnd);
		rep.saveStepAttribute(id_transformation, id_step, "batchSize", this.batchSize);
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		this.outputType = rep.getStepAttributeString(id_step, "outputType");
		this.url = rep.getStepAttributeString(id_step, "url");
		this.clusterName = rep.getStepAttributeString(id_step, "clusterName");
		this.database = rep.getStepAttributeString(id_step, "database");
		this.outputTable = rep.getStepAttributeString(id_step, "outputTable");
		this.fieldName = rep.getStepAttributeString(id_step, "fieldName");
		this.statFieldName = rep.getStepAttributeString(id_step, "statFieldName");
		this.statCnd = rep.getStepAttributeString(id_step, "statCnd");
		this.batchSize = rep.getStepAttributeString(id_step, "batchSize");
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transMeta, 
			StepMeta stepMeta, RowMetaInterface prev,
			String[] input, String[] output, RowMetaInterface info) {
		CheckResult cr = new CheckResult(1,
				BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.AcceptFilenamesOk", 
						new String[0]), stepMeta);
		remarks.add(cr);
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
			TransMeta transMeta, Trans trans) {
		return new EDBOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new EDBOutData();
	}

	
	public String saveToOracle(List<Map<String, Object>> olist) {
		Connection con = null;
		try {
			// url= ip:port:orcl?user=root&pwd=123456
			String[] info = this.url.split("[?]");
			String url = "jdbc:oracle:thin:@" + info[0];
			String[] userpwd = info[1].split("&");
			String user = userpwd[0].split("=")[1];
			String password = userpwd[1].split("=")[1];
			logBasic("connect to oracle:"+user+"&"+password);
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(url, user, password);
			Statement smt = con.createStatement();
			//表结构信息
			Map<String, String> fieldsInfo = getFieldsInfo(con, outputTable);
			//如果获取元数据失败，则从配置文件中获取
			if(fieldsInfo.size()==0){
				logBasic("====dateFields:"+dateFields);
				try {
					String[] dfs = dateFields.split(",");
					for (String df : dfs) {
						String[] kv = df.split("[:]");
						fieldsInfo.put(kv[0], kv[1]);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			// 关闭事务自动提交
			con.setAutoCommit(false);
			Long startTime = System.currentTimeMillis();
			int i = 0;
			logBasic("开始组装sql...");
			for (Map<String, Object> row : olist) {
				Iterator<Entry<String, Object>> iter = row.entrySet().iterator();
				String format = "yyyy-MM-dd HH24:mi:ss";
				String fields = "";
				String values = "";
				while (iter.hasNext()) {
					Entry<String, Object> item = iter.next();
					String key = item.getKey();
					Object val = item.getValue();
					fields += "," + key;
					String fieldType = fieldsInfo.get(key);
					if(fieldType==null) fieldType = "";
					switch (fieldType.toUpperCase()) {
						case "DATE":
							values += ",TO_DATE('" + val + "','"+format+"')";
							break;
						case "TIMESTAMP":
							format = "yyyy-MM-dd HH24:mi:ss:ff";
							values += ",TO_TIMESTAMP('" + val + "','"+format+"')";
							break;
						default:
							values += ",'" + val + "'";
							break;
					}
				}
				fields = " (" + fields.substring(1) + ")";
				values = " (" + values.substring(1) + ")";
				String sql = "INSERT INTO " + outputTable + fields + " VALUES " + values;
				if(i++ == 0){
					logBasic("====sql:"+sql);
				}
				// 把一个SQL命令加入命令列表
				smt.addBatch(sql);
			}
			// 执行批量新增
			smt.executeBatch();
			// 语句执行完毕，提交本事务
			con.commit();
			Long endTime = System.currentTimeMillis();
			logBasic("write " + olist.size() + " records to Oracle, use：" + (endTime - startTime));
			smt.close();
			con.close();

		} catch (Exception e) {
			try {
				if (con != null)
					con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			logBasic("ChinacloudException:" + e);
			e.printStackTrace();
			return "Error";
		}

		return "OK";
	}
	private Map<String, String> getFieldsInfo(Connection con, String outputTable) {
		logBasic("查询表["+outputTable+"]的元数据信息...");
		Map<String, String> map = new HashMap<String, String>();
		Statement st;
		try {
			st = con.createStatement();
			ResultSet result = st.executeQuery("select * from "+outputTable+" where rownum=1");
	        ResultSetMetaData rsmd = result.getMetaData();
	        int nrCols = rsmd.getColumnCount();
	        for (int i = 1; i <= nrCols; i++) {
	    		map.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(i));
	    	}
		} catch (SQLException e) {
			logBasic("查询表["+outputTable+"]的元数据信息出错！");
			e.printStackTrace();
		}
		
		return map;
	}
	


	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getJobId() {
		return this.jobId;
	}
	public void setCheckColumn(String checkColumn) {
		this.checkColumn = checkColumn;
	}	
	public String getCheckColumn() {
		return this.checkColumn;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public String getEnd() {
		return this.end;
	}
	public void setUseLastValue(String useLastValue) {
		this.useLastValue = useLastValue;
	}
	public String getUseLastValue() {
		return this.useLastValue;
	}
	public void setDriverName(String driverName) {
		if(Strings.isNullOrEmpty(driverName))
			driverName = "com.mysql.jdbc.Driver";
		this.driverName = driverName;
	}
	public String getDriverName() {
		return driverName;
	}
	public void setMysqlUrl(String mysqlUrl) {
		this.mysqlUrl = mysqlUrl;
	}
	public String getMysqlUrl() {
		return mysqlUrl;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getUsername() {
		return username;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getPassword() {
		return password;
	}
	public void setTable(String table) {
		if(Strings.isNullOrEmpty(table))
			table = "job";
		this.table = table;
	}
	public String getTable() {
		return table;
	}
	public void setUpdateField(String updateField) {
		if(Strings.isNullOrEmpty(updateField))
			updateField = "lastModified";
		this.updateField = updateField;
	}
	public String getUpdateField() {
		return updateField;
	}
	public void setDateFields(String dateFields) {
		this.dateFields = dateFields;
	}
	public String getDateFields() {
		return dateFields;
	}
	
	/////////////////////
	
	public String getUrl() {
		return this.url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getOutputType() {
		return outputType;
	}
	public void setOutputType(String outputType) {
		this.outputType = outputType;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getOutputTable() {
		return outputTable;
	}
	public void setOutputTable(String outputTable) {
		this.outputTable = outputTable;
	}
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String getStatFieldName() {
		return statFieldName;
	}
	public void setStatFieldName(String statFieldName) {
		this.statFieldName = statFieldName;
	}
	public String getStatCnd() {
		return statCnd;
	}
	public void setStatCnd(String statCnd) {
		this.statCnd = statCnd;
	}
	public String getClusterName() {
		return clusterName;
	}
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	public String getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(String batchSize) {
		this.batchSize = batchSize;
	}
	
}
