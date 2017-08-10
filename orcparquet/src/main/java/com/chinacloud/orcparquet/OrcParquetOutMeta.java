package com.chinacloud.orcparquet;

import com.google.common.base.Strings;
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
import org.pentaho.di.trans.step.*;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

@Step(
	id = "OrcParquetOutputPlugin", 
	image = "orcparquet-output.png", 
	i18nPackageName = "com.chinacloud.orcparquet", 
	name = "Plugin.Name", 
	description = "Plugin.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class OrcParquetOutMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = OrcParquetOutMeta.class;
	
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
	private String fileName;
	private boolean cleanOutput;
	private String outputType;
	private String url;
	private String blockSize;
	private String pageSize;
	private boolean createAndLoad;
	private String hiveVersion;
/*	private String hiveHost;
	private String hivePort;
	private String hiveDB;*/
	private String hiveUrl;
	private String hiveUser;
	private String hivePassword;
	private String hiveTable;
	private boolean overwriteTable;
	private boolean excuteSql;
	private String sqlContent;

	public void setDefault() {
		this.fileName = "";
		this.cleanOutput = true;
		this.outputType = "parquet";
		this.url = "172.16.50.24:8020;172.16.50.21:8020";
		this.blockSize = "128";
		this.pageSize = "1024";
		this.createAndLoad = false;
		this.hiveVersion = "impala";
/*		this.hiveHost = "";
		this.hivePort = "";
		this.hiveDB = "";*/
		this.hiveUrl = "jdbc:hive2://172.16.50.21:21050/test;auth=noSasl";
		this.hiveUser = "";
		this.hivePassword = "";
		this.hiveTable = "";
		this.overwriteTable = false;
		this.setExcuteSql(false);
		this.setSqlContent("");
	}

	public String getXML() throws KettleValueException {
		StringBuilder retval = new StringBuilder();
		retval.append("    ").append(XMLHandler.addTagValue("fileName", this.fileName));
		retval.append("    ").append(XMLHandler.addTagValue("cleanOutput", this.cleanOutput));
		retval.append("    ").append(XMLHandler.addTagValue("outputType", this.outputType));
		retval.append("    ").append(XMLHandler.addTagValue("url", this.url));
		retval.append("    ").append(XMLHandler.addTagValue("blockSize", this.blockSize));
		retval.append("    ").append(XMLHandler.addTagValue("pageSize", this.pageSize));
		retval.append("    ").append(XMLHandler.addTagValue("createAndLoad", this.createAndLoad));
		retval.append("    ").append(XMLHandler.addTagValue("hiveVersion", this.hiveVersion));
		/*retval.append("    ").append(XMLHandler.addTagValue("hiveHost", this.hiveHost));
		retval.append("    ").append(XMLHandler.addTagValue("hivePort", this.hivePort));
		retval.append("    ").append(XMLHandler.addTagValue("hiveDB", this.hiveDB));*/
		retval.append("    ").append(XMLHandler.addTagValue("hiveUrl", this.hiveUrl));
		retval.append("    ").append(XMLHandler.addTagValue("hiveUser", this.hiveUser));
		retval.append("    ").append(XMLHandler.addTagValue("hivePassword", this.hivePassword));
		retval.append("    ").append(XMLHandler.addTagValue("hiveTable", this.hiveTable));
		retval.append("    ").append(XMLHandler.addTagValue("overwriteTable", this.overwriteTable));
		retval.append("    ").append(XMLHandler.addTagValue("excuteSql", this.excuteSql));
		retval.append("    ").append(XMLHandler.addTagValue("sqlContent", this.sqlContent));
		
		return retval.toString();
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {
		this.fileName = XMLHandler.getTagValue(stepnode, "fileName");
		this.cleanOutput = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "cleanOutput"));
		this.outputType = XMLHandler.getTagValue(stepnode, "outputType");
		this.url = XMLHandler.getTagValue(stepnode, "url");
		this.blockSize = XMLHandler.getTagValue(stepnode, "blockSize");
		this.pageSize = XMLHandler.getTagValue(stepnode, "pageSize");
		this.createAndLoad = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "createAndLoad"));
		this.hiveVersion = XMLHandler.getTagValue(stepnode, "hiveVersion");
		/*this.hiveHost = XMLHandler.getTagValue(stepnode, "hiveHost");
		this.hivePort = XMLHandler.getTagValue(stepnode, "hivePort");
		this.hiveDB = XMLHandler.getTagValue(stepnode, "hiveDB");*/
		this.hiveUrl = XMLHandler.getTagValue(stepnode, "hiveUrl");
		this.hiveUser = XMLHandler.getTagValue(stepnode, "hiveUser");
		this.hivePassword = XMLHandler.getTagValue(stepnode, "hivePassword");
		this.hiveTable = XMLHandler.getTagValue(stepnode, "hiveTable");
		this.overwriteTable = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "overwriteTable"));
		this.excuteSql = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "excuteSql"));
		this.sqlContent = XMLHandler.getTagValue(stepnode, "sqlContent");
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "fileName", this.fileName);
		rep.saveStepAttribute(id_transformation, id_step, "cleanOutput", this.cleanOutput);
		rep.saveStepAttribute(id_transformation, id_step, "outputType", this.outputType);
		rep.saveStepAttribute(id_transformation, id_step, "url", this.url);
		rep.saveStepAttribute(id_transformation, id_step, "blockSize", this.blockSize);
		rep.saveStepAttribute(id_transformation, id_step, "pageSize", this.pageSize);
		rep.saveStepAttribute(id_transformation, id_step, "createAndLoad", this.createAndLoad);
		rep.saveStepAttribute(id_transformation, id_step, "hiveVersion", this.hiveVersion);
		/*rep.saveStepAttribute(id_transformation, id_step, "hiveHost", this.hiveHost);
		rep.saveStepAttribute(id_transformation, id_step, "hivePort", this.hivePort);
		rep.saveStepAttribute(id_transformation, id_step, "hiveDB", this.hiveDB);*/
		rep.saveStepAttribute(id_transformation, id_step, "hiveUrl", this.hiveUrl);
		rep.saveStepAttribute(id_transformation, id_step, "hiveUser", this.hiveUser);
		rep.saveStepAttribute(id_transformation, id_step, "hivePassword", this.hivePassword);
		rep.saveStepAttribute(id_transformation, id_step, "hiveTable", this.hiveTable);
		rep.saveStepAttribute(id_transformation, id_step, "overwriteTable", this.overwriteTable);
		rep.saveStepAttribute(id_transformation, id_step, "excuteSql", this.excuteSql);
		rep.saveStepAttribute(id_transformation, id_step, "sqlContent", this.sqlContent);
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		this.fileName = rep.getStepAttributeString(id_step, "fileName");
		this.cleanOutput = rep.getStepAttributeBoolean(id_step, "cleanOutput");
		this.outputType = rep.getStepAttributeString(id_step, "outputType");
		this.url = rep.getStepAttributeString(id_step, "url");
		this.blockSize = rep.getStepAttributeString(id_step, "blockSize");
		this.pageSize = rep.getStepAttributeString(id_step, "pageSize");
		this.createAndLoad = rep.getStepAttributeBoolean(id_step, "createAndLoad");
		this.hiveVersion = rep.getStepAttributeString(id_step, "hiveVersion");
		/*this.hiveHost = rep.getStepAttributeString(id_step, "hiveHost");
		this.hivePort = rep.getStepAttributeString(id_step, "hivePort");
		this.hiveDB = rep.getStepAttributeString(id_step, "hiveDB");*/
		this.hiveUrl = rep.getStepAttributeString(id_step, "hiveUrl");
		this.hiveUser = rep.getStepAttributeString(id_step, "hiveUser");
		this.hivePassword = rep.getStepAttributeString(id_step, "hivePassword");
		this.hiveTable = rep.getStepAttributeString(id_step, "hiveTable");
		this.overwriteTable = rep.getStepAttributeBoolean(id_step, "overwriteTable");
		this.excuteSql = rep.getStepAttributeBoolean(id_step, "excuteSql");
		this.sqlContent = rep.getStepAttributeString(id_step, "sqlContent");
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
		return new OrcParquetOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new OrcParquetOutData();
	}

	
	///////////////
	
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
	
	//////////

	public String getFileName() {
		return replaceNull(fileName);
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public boolean isCleanOutput() {
		return cleanOutput;
	}
	public void setCleanOutput(boolean cleanOutput) {
		this.cleanOutput = cleanOutput;
	}
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
	public String getBlockSize() {
		if(blockSize == null)
			blockSize = "128";
		return blockSize;
	}
	public void setBlockSize(String blockSize) {
		this.blockSize = blockSize;
	}
	public String getPageSize() {
		if(pageSize == null)
			pageSize = "1024";
		return pageSize;
	}
	public void setPageSize(String pageSize) {
		this.pageSize = pageSize;
	}
	public boolean isCreateAndLoad() {
		return createAndLoad;
	}
	public void setCreateAndLoad(boolean createAndLoad) {
		this.createAndLoad = createAndLoad;
	}
	public String getHiveVersion() {
		return replaceNull(hiveVersion);
	}
	public void setHiveVersion(String hiveVersion) {
		this.hiveVersion = hiveVersion;
	}
	/*public String getHiveHost() {
		return replaceNull(hiveHost);
	}
	public void setHiveHost(String hiveHost) {
		this.hiveHost = hiveHost;
	}
	public String getHivePort() {
		return replaceNull(hivePort);
	}
	public void setHivePort(String hivePort) {
		this.hivePort = hivePort;
	}
	public String getHiveDB() {
		return replaceNull(hiveDB);
	}
	public void setHiveDB(String hiveDB) {
		this.hiveDB = hiveDB;
	}*/
	public String getHiveUrl() {
		return this.hiveUrl;
	}
	public void setHiveUrl(String hiveUrl) {
		this.hiveUrl = hiveUrl;
	}
	public String getHiveUser() {
		return replaceNull(hiveUser);
	}
	public void setHiveUser(String hiveUser) {
		this.hiveUser = hiveUser;
	}
	public String getHivePassword() {
		return replaceNull(hivePassword);
	}
	public void setHivePassword(String hivePassword) {
		this.hivePassword = hivePassword;
	}
	public String getHiveTable() {
		return replaceNull(hiveTable);
	}
	public void setHiveTable(String hiveTable) {
		this.hiveTable = hiveTable;
	}

	public boolean isOverwriteTable() {
		return overwriteTable;
	}

	public void setOverwriteTable(boolean overwriteTable) {
		this.overwriteTable = overwriteTable;
	}
	
	private String replaceNull(String value) {
		return value==null? "":value;
	}

	public boolean getExcuteSql() {
		return excuteSql;
	}

	public void setExcuteSql(boolean excuteSql) {
		this.excuteSql = excuteSql;
	}

	public String getSqlContent() {
		return replaceNull(sqlContent);
	}

	public void setSqlContent(String sqlContent) {
		this.sqlContent = sqlContent;
	}

}
