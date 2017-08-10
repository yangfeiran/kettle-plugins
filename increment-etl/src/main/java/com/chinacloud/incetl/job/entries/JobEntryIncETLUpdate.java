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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.nutz.dao.Dao;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
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
 * 增量抽取作业更新插件
 *
 * @author nivalsoul
 */
@JobEntry(
	id = "IncETLUpdateJobPlugin", 
	image = "incetl-out.png", 
	i18nPackageName = "com.chinacloud.incetl.job.entries", 
	name = "PluginUpdate.Name", 
	description = "PluginUpdate.Description", 
	categoryDescription = "Category.Description", 
	documentationUrl = "http://www.chinacloud.com.cn"
)
public class JobEntryIncETLUpdate extends JobEntryBase implements Cloneable, JobEntryInterface {
	private static Class<?> PKG = JobEntryIncETLUpdate.class; 

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


	public JobEntryIncETLUpdate(String n) {
		super(n, "");
	}

	public JobEntryIncETLUpdate() {
		this("");
	}

	public Object clone() {
		JobEntryIncETLUpdate je = (JobEntryIncETLUpdate) super.clone();
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

		return retval.toString();
	}

	public void loadXML(Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep,
			IMetaStore metaStore) throws KettleXMLException {
		try {
			super.loadXML(entrynode, databases, slaveServers);
		} catch (KettleXMLException xe) {
			throw new KettleXMLException("Unable to load job entry of type '" + jobEnrtyType + "' from XML node", xe);
		}
	}

	public void loadRep(Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
			List<SlaveServer> slaveServers) throws KettleException {
		;
	}

	public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_job) throws KettleException {
		;
	}

	public Result execute(Result result, int nr) throws KettleException {
		result.setResult(true);
		result.setNrErrors(0);
		try {
			logBasic("====更新增量信息...");
			getUserVariabls();
			
			jobId = getVariable("jobId");
			logBasic("****update job by jobId:"+jobId);
			List<RowMetaAndData> rows = new ArrayList<RowMetaAndData>( result.getRows() );
			//经过升序排序后，获取最后一行的数据即为最新一条
			if(rows.size()>0){
				String lastEndValue = rows.get(rows.size()-1).getString(checkColumn, "");
				logBasic("====取到最后更新值:"+lastEndValue);
				lastEndValue = lastEndValue.replaceAll("00Z", "000");
				if(!Strings.isNullOrEmpty(end)){//有end参数，表示按时间或者分段抽取
					if(!"true".equals(useLastValue)){
						lastEndValue = end;
					}
				}
				updateJobInfo(lastEndValue);
			}
		} catch (Exception e) {
			result.setResult(false);
			result.setNrErrors(1);
			logError("init job plugin '"+jobEnrtyType+"' faild, exception: " + e.getMessage());
		}

		return result;
	}
	
	private void updateJobInfo(String lastEndValue) {
		try {
			DaoFactory.init(driverName, mysqlUrl, username, password);
			Dao dao = DaoFactory.getDao();
			JobSchedule js = dao.fetch(JobSchedule.class, jobId);
			if(js!=null){
				if("lastmodify".equals(Constant.incType.get(js.getIncType()))){
					//根据实际数据生成时间格式字符串
					StringBuffer sb = new StringBuffer("yyyy-MM-dd HH:mm:ss");
					sb.setCharAt(4, lastEndValue.charAt(4));
					sb.setCharAt(7, lastEndValue.charAt(7));
					if(lastEndValue.length()>10 && lastEndValue.charAt(10)=='T'){
						sb.replace(10, 11, "'T'");
					}
					if(lastEndValue.length()>19 && lastEndValue.charAt(19)=='.'){
						sb.append(".SSS");
						if(lastEndValue.length()>23){
							lastEndValue = lastEndValue.substring(0, 23);
						}
					}
					DateFormat srcDf = new SimpleDateFormat(sb.toString());
					DateFormat desDf = new SimpleDateFormat(js.getDataFormat());
					lastEndValue = desDf.format(srcDf.parse(lastEndValue));
				}
			}
			js.setLastStartValue(js.getLastEndValue());
			js.setLastEndValue(lastEndValue);
			js.setLastRunTime(new Timestamp(new Date().getTime()));
			dao.update(js);
		} catch (Exception e) {
			logBasic("ChinacloudException:UpdateJobInfoFailed,"+e.getMessage());
		}
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
	
}
