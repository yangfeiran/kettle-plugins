package com.chinacloud.incetl.trans.step;

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
	id = "IncETLInputPlugin", 
	image = "incetl.png", 
	i18nPackageName = "com.chinacloud.incetl.trans.step", 
	name = "Plugin.Name", 
	description = "Plugin.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class IncETLInputMeta extends BaseStepMeta implements StepMetaInterface {

	private static Class<?> PKG = IncETLInputMeta.class;
	
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
	
	/*****界面变量*****/
	private String incType;
	private String incField;
	private String dataFormat;
	private String startValue;
	private String endValue;

	public void setDefault() {
		this.incType = "时间戳";
		this.incField = "";
		this.dataFormat = "yyyy-MM-dd HH:mm:ss";
		this.startValue = "";
		this.endValue = "";
	}

	public String getXML() throws KettleValueException {
		StringBuilder retval = new StringBuilder();
		retval.append("    ").append(XMLHandler.addTagValue("incType", this.incType));
		retval.append("    ").append(XMLHandler.addTagValue("incField", this.incField));
		retval.append("    ").append(XMLHandler.addTagValue("dataFormat", this.dataFormat));
		retval.append("    ").append(XMLHandler.addTagValue("startValue", this.startValue));
		retval.append("    ").append(XMLHandler.addTagValue("endValue", this.endValue));
		return retval.toString();
	}

	public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleXMLException {
		this.incType = XMLHandler.getTagValue(stepnode, "incType");
		this.incField = XMLHandler.getTagValue(stepnode, "incField");
		this.dataFormat = XMLHandler.getTagValue(stepnode, "dataFormat");
		this.startValue = XMLHandler.getTagValue(stepnode, "startValue");
		this.endValue = XMLHandler.getTagValue(stepnode, "endValue");
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		rep.saveStepAttribute(id_transformation, id_step, "incType", this.incType);
		rep.saveStepAttribute(id_transformation, id_step, "incField", this.incField);
		rep.saveStepAttribute(id_transformation, id_step, "dataFormat", this.dataFormat);
		rep.saveStepAttribute(id_transformation, id_step, "startValue", this.startValue);
		rep.saveStepAttribute(id_transformation, id_step, "endValue", this.endValue);
	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		this.incType = rep.getStepAttributeString(id_step, "incType");
		this.incField = rep.getStepAttributeString(id_step, "incField");
		this.dataFormat = rep.getStepAttributeString(id_step, "dataFormat");
		this.startValue = rep.getStepAttributeString(id_step, "startValue");
		this.endValue = rep.getStepAttributeString(id_step, "endValue");
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
		return new IncETLInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public StepDataInterface getStepData() {
		return new IncETLInputData();
	}


	
	////////////////////////////////
	////////////////////////////////
	
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getJobId() {
		return jobId;
	}
	
	public void setCheckColumn(String checkColumn) {
		this.checkColumn = checkColumn;
	}
	public String getCheckColumn() {
		return checkColumn;
	}
	
	public void setEnd(String end) {
		this.end = end;
	}
	public String getEnd() {
		return end;
	}
	
	public void setUseLastValue(String useLastValue) {
		this.useLastValue = useLastValue;
	}
	public String getUseLastValue() {
		return useLastValue;
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

	
	/////////////////////
	
	public String getIncType() {
		return incType;
	}
	
	public void setIncType(String incType) {
		this.incType = incType;
	}
	
	public String getIncField() {
		if(incField == null) 
			incField = "";
		return incField;
	}
	
	public void setIncField(String incField) {
		this.incField = incField;
	}
	
	public String getDataFormat() {
		if(dataFormat == null) 
			dataFormat = "yyyy-MM-dd HH:mm:ss";
		return dataFormat;
	}
	
	public void setDataFormat(String dataFormat) {
		this.dataFormat = dataFormat;
	}
	
	public String getStartValue() {
		if(startValue == null) 
			startValue = "";
		return startValue;
	}
	
	public void setStartValue(String startValue) {
		this.startValue = startValue;
	}
	
	public String getEndValue() {
		if(endValue == null) 
			endValue = "";
		return endValue;
	}
	
	public void setEndValue(String endValue) {
		this.endValue = endValue;
	}
	
}
