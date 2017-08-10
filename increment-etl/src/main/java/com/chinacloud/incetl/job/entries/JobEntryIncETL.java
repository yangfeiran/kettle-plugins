/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.chinacloud.incetl.job.entries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.nutz.dao.Dao;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.chinacloud.incetl.Constant;
import com.chinacloud.incetl.DaoFactory;
import com.chinacloud.incetl.JobSchedule;
import com.chinacloud.incetl.MD5;
import com.google.common.base.Strings;

/**
 * 增量抽取作业插件，设置初始条件值
 *
 * @author nivalsoul
 */
@JobEntry(
	id = "IncETLJobPlugin", 
	image = "incetl-in.png", 
	i18nPackageName = "com.chinacloud.incetl.job.entries", 
	name = "Plugin.Name", 
	description = "Plugin.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class JobEntryIncETL extends JobEntryBase implements Cloneable, JobEntryInterface {
	private static Class<?> PKG = JobEntryIncETL.class; 

	private String jobEnrtyType = "IncETL";
	private String jobName = "";

	/***** 命名参数和环境变量 *****/
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

	/***** 界面变量 *****/
	private String incType;
	private String incField;
	private String dataFormat;
	private String startValue;
	private String endValue;

	public JobEntryIncETL(String n) {
		super(n, "");
		this.incType = "时间戳";
		this.incField = "";
		this.dataFormat = "yyyy-MM-dd HH:mm:ss";
		this.startValue = "";
		this.endValue = "";
	}

	public JobEntryIncETL() {
		this("");
	}

	public Object clone() {
		JobEntryIncETL je = (JobEntryIncETL) super.clone();
		return je;
	}
	
	public boolean evaluates() {
        return true ;
    }

    public boolean isUnconditional() {
        return true ;
    }

	public String getXML() {
		StringBuffer retval = new StringBuffer(300);
		retval.append(super.getXML());
		retval.append("    ").append(XMLHandler.addTagValue("incType", this.incType));
		retval.append("    ").append(XMLHandler.addTagValue("incField", this.incField));
		retval.append("    ").append(XMLHandler.addTagValue("dataFormat", this.dataFormat));
		retval.append("    ").append(XMLHandler.addTagValue("startValue", this.startValue));
		retval.append("    ").append(XMLHandler.addTagValue("endValue", this.endValue));

		return retval.toString();
	}

	public void loadXML(Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
			IMetaStore metaStore) throws KettleXMLException {
		try {
			super.loadXML(entrynode, databases, slaveServers);
			this.incType = XMLHandler.getTagValue(entrynode, "incType");
			this.incField = XMLHandler.getTagValue(entrynode, "incField");
			this.dataFormat = XMLHandler.getTagValue(entrynode, "dataFormat");
			this.startValue = XMLHandler.getTagValue(entrynode, "startValue");
			this.endValue = XMLHandler.getTagValue(entrynode, "endValue");
		} catch (KettleXMLException xe) {
			throw new KettleXMLException("Unable to load job entry of type '" + jobEnrtyType + "' from XML node", xe);
		}
	}

	public void loadRep(Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
			List<SlaveServer> slaveServers) throws KettleException {
		try {
			this.incType = rep.getStepAttributeString(id_jobentry, "incType");
			this.incField = rep.getStepAttributeString(id_jobentry, "incField");
			this.dataFormat = rep.getStepAttributeString(id_jobentry, "dataFormat");
			this.startValue = rep.getStepAttributeString(id_jobentry, "startValue");
			this.endValue = rep.getStepAttributeString(id_jobentry, "endValue");
		} catch (KettleException dbe) {
			throw new KettleException("Unable to load job entry of type '" + jobEnrtyType + "' from the repository for id_jobentry=" + id_jobentry, dbe);
		}
	}

	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_job) throws KettleException {
		try {
			rep.saveStepAttribute(id_job, getObjectId(), "incType", this.incType);
			rep.saveStepAttribute(id_job, getObjectId(), "incField", this.incField);
			rep.saveStepAttribute(id_job, getObjectId(), "dataFormat", this.dataFormat);
			rep.saveStepAttribute(id_job, getObjectId(), "startValue", this.startValue);
			rep.saveStepAttribute(id_job, getObjectId(), "endValue", this.endValue);

		} catch (KettleDatabaseException dbe) {
			throw new KettleException("Unable to save job entry of type '" + jobEnrtyType + "' to the repository for id_job=" + id_job, dbe);
		}
	}

	public Result execute(Result result, int nr) throws KettleException {
		result.setResult(true);
		result.setNrErrors(0);
		try {
			logBasic("====设置初始值...");
			Job parentJob = getParentJob();
			while (parentJob != null) {
				jobName = parentJob.getJobname();
				parentJob = parentJob.getParentJob();
			}
			
			logBasic("jobName==" + jobName);
			jobId = MD5.getHashString(Const.getIPAddress()+jobName);
			getUserVariabls();
			setUserVariable("jobId", jobId);
			setInitValue();

		} catch (Exception e) {
			result.setResult(false);
			result.setNrErrors(1);
			logError("init job plugin '"+jobEnrtyType+"' faild, exception: " + e.getMessage());
		}

		return result;
	}
	
	private void getUserVariabls() {
		setCheckColumn(getVariable("checkColumn"));
    	setEnd(getVariable("end"));
    	setUseLastValue(getVariable("useLastValue"));
    	setDriverName(getVariable("driver"));
    	setMysqlUrl(getVariable("url"));
    	setUsername(getVariable("username"));
    	setPassword(getVariable("password"));
    	setTable(getVariable("table"));
    	setUpdateField(getVariable("updateField"));
	}

	private void setInitValue() throws Exception {
		String dataFormat = getDataFormat();
		DateFormat df = new SimpleDateFormat(dataFormat);
		String end = df.format(new Date());
		try {
			DaoFactory.init(driverName, mysqlUrl, username, password);
			Dao dao = DaoFactory.getDao();
			JobSchedule js = dao.fetch(JobSchedule.class, jobId);
			if(js!=null){
				setUserVariable("start", js.getLastEndValue());
				if ("lastmodify".equals(Constant.incType.get(js.getIncType()))) {
					setUserVariable("end", end);
				}
			}else{
				if(Strings.isNullOrEmpty(getStartValue())){
					throw new IllegalArgumentException("起始值为空，请设置！");
				}
				String endValue = getEndValue();
				if ("lastmodify".equals(Constant.incType.get(getIncType())) && Strings.isNullOrEmpty(endValue)) {
					endValue = end;
				}
				js = new JobSchedule();
				js.setJobId(jobId);
				js.setJobName(jobName);
				js.setIncType(getIncType());
				js.setIncField(getIncField());
				js.setDataFormat(getDataFormat());
				js.setLastStartValue(getStartValue());
				//新增时end和start一样，以便后续的更新使用
				js.setLastEndValue(getStartValue());
				dao.insert(js);
				
				setUserVariable("start", getStartValue());
				//新增时始终这是end参数
				setUserVariable("end", endValue);
			}
			setUserVariable("checkColumn", getIncField());
		} catch (Exception e) {
			logError("ChinacloudException:SetInitValueFailed," + e.getMessage());
			throw e;
		}
	}

	private void setUserVariable(String varname, String value) {
		setVariable(varname, value);
		Job parentJob = getParentJob();
		while (parentJob != null) {
			parentJob.setVariable(varname, value);
			parentJob = parentJob.getParentJob();
		}

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
		if (Strings.isNullOrEmpty(driverName))
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
		if (Strings.isNullOrEmpty(table))
			table = "job";
		this.table = table;
	}

	public String getTable() {
		return table;
	}

	public void setUpdateField(String updateField) {
		if (Strings.isNullOrEmpty(updateField))
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
		if (incField == null)
			incField = "";
		return incField;
	}

	public void setIncField(String incField) {
		this.incField = incField;
	}

	public String getDataFormat() {
		if (dataFormat == null)
			dataFormat = "yyyy-MM-dd HH:mm:ss";
		return dataFormat;
	}

	public void setDataFormat(String dataFormat) {
		this.dataFormat = dataFormat;
	}

	public String getStartValue() {
		if (startValue == null)
			startValue = "";
		return startValue;
	}

	public void setStartValue(String startValue) {
		this.startValue = startValue;
	}

	public String getEndValue() {
		if (endValue == null)
			endValue = "";
		return endValue;
	}

	public void setEndValue(String endValue) {
		this.endValue = endValue;
	}
	
}
