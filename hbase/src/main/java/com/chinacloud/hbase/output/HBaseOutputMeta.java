package com.chinacloud.hbase.output;

import java.util.List;
import java.util.Map;

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
	id = "HBaseOutputPlugin", 
	image = "hbase-icon.png", 
	i18nPackageName = "com.chinacloud.hbase.output", 
	name = "Plugin.Name", 
	description = "Plugin.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class HBaseOutputMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = HBaseOutputMeta.class;
	
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
	private String zkHostPorts;
	private String hbaseMaster;
	private String hbaseRootdir;
	private String tableName;
	private String familyName;
	private String batchSize;
	

	public void setDefault() {
		this.zkHostPorts = "ip1:2181;ip2:2181";
		this.hbaseMaster = "localhost:60000";
		this.hbaseRootdir = "hdfs://localhost:8020/hbase";
		this.tableName = "";
		this.familyName = "";
		this.batchSize = "5000";
	}

	public String getXML() throws KettleValueException {
		StringBuilder retval = new StringBuilder();
		retval.append("    ").append(XMLHandler.addTagValue("zkHostPorts", this.zkHostPorts));
		retval.append("    ").append(XMLHandler.addTagValue("hbaseMaster", this.hbaseMaster));
		retval.append("    ").append(XMLHandler.addTagValue("hbaseRootdir", this.hbaseRootdir));
		retval.append("    ").append(XMLHandler.addTagValue("tableName", this.tableName));
		retval.append("    ").append(XMLHandler.addTagValue("familyName", this.familyName));
		retval.append("    ").append(XMLHandler.addTagValue("batchSize", this.batchSize));
		
		return retval.toString();
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {
		this.zkHostPorts = XMLHandler.getTagValue(stepnode, "zkHostPorts");
		this.hbaseMaster = XMLHandler.getTagValue(stepnode, "hbaseMaster");
		this.hbaseRootdir = XMLHandler.getTagValue(stepnode, "hbaseRootdir");
		this.tableName = XMLHandler.getTagValue(stepnode, "tableName");
		this.familyName = XMLHandler.getTagValue(stepnode, "familyName");
		this.batchSize = XMLHandler.getTagValue(stepnode, "batchSize");
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "zkHostPorts", this.zkHostPorts);
		rep.saveStepAttribute(id_transformation, id_step, "hbaseMaster", this.hbaseMaster);
		rep.saveStepAttribute(id_transformation, id_step, "hbaseRootdir", this.hbaseRootdir);
		rep.saveStepAttribute(id_transformation, id_step, "tableName", this.tableName);
		rep.saveStepAttribute(id_transformation, id_step, "familyName", this.familyName);
		rep.saveStepAttribute(id_transformation, id_step, "batchSize", this.batchSize);
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		this.zkHostPorts = rep.getStepAttributeString(id_step, "zkHostPorts");
		this.hbaseMaster = rep.getStepAttributeString(id_step, "hbaseMaster");
		this.hbaseRootdir = rep.getStepAttributeString(id_step, "hbaseRootdir");
		this.tableName = rep.getStepAttributeString(id_step, "tableName");
		this.familyName = rep.getStepAttributeString(id_step, "familyName");
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
		return new HBaseOutput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new HBaseOutputData();
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

	public String getZkHostPorts() {
		return zkHostPorts;
	}

	public void setZkHostPorts(String zkHostPorts) {
		this.zkHostPorts = zkHostPorts;
	}

	public String getHbaseMaster() {
		return hbaseMaster;
	}

	public void setHbaseMaster(String hbaseMaster) {
		this.hbaseMaster = hbaseMaster;
	}

	public String getHbaseRootdir() {
		return hbaseRootdir;
	}

	public void setHbaseRootdir(String hbaseRootdir) {
		this.hbaseRootdir = hbaseRootdir;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}
	
	public String getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(String batchSize) {
		this.batchSize = batchSize;
	}
	
	//////////


}
