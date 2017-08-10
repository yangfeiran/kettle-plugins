package com.chinacloud.incetl.trans.step;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.nutz.dao.Dao;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.chinacloud.incetl.Constant;
import com.chinacloud.incetl.DaoFactory;
import com.chinacloud.incetl.JobSchedule;
import com.chinacloud.incetl.MD5;
import com.google.common.base.Strings;


public class IncETLInput extends BaseStep implements StepInterface {
    private IncETLInputData data;
    private IncETLInputMeta meta;
    private String jobId;
    private String jobName;
    
    public IncETLInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((IncETLInputMeta) smi);
        this.data = ((IncETLInputData) sdi);
        Object[] row = getRow();
        RowMetaInterface rowMeta = getInputRowMeta();
        
        if(rowMeta==null){
            setOutputDone();
            return false;
        }
        if (row == null) {//没有数据需要处理了
            
            return false;
        }  
		
        if (this.first) {
            first = false;
            this.data.outputRowMeta = getInputRowMeta().clone();
            this.meta.getFields(this.data.outputRowMeta, getStepname(), null, null, this);
            logBasic("****first.....");
        }
        
        putRow(this.data.outputRowMeta, row);
        return true;
    }
   
    private void setInitValue() throws Exception {
		String dataFormat = meta.getDataFormat();
		DateFormat df = new SimpleDateFormat(dataFormat);
		String end = df.format(new Date());
		try {
			DaoFactory.init(meta.getDriverName(), meta.getMysqlUrl(), meta.getUsername(), meta.getPassword());
			Dao dao = DaoFactory.getDao();
			JobSchedule js = dao.fetch(JobSchedule.class, jobId);
			if(js!=null){
				setUserVariable("start", js.getLastEndValue());
				if ("lastmodify".equals(Constant.incType.get(js.getIncType()))) {
					setUserVariable("end", end);
				}
			}else{
				if(Strings.isNullOrEmpty(meta.getStartValue())){
					throw new IllegalArgumentException("起始值为空，请设置！");
				}
				String endValue = meta.getEndValue();
				if ("lastmodify".equals(Constant.incType.get(meta.getIncType())) && Strings.isNullOrEmpty(endValue)) {
					endValue = end;
				}
				js = new JobSchedule();
				js.setJobId(jobId);
				js.setJobName(jobName);
				js.setIncType(meta.getIncType());
				js.setIncField(meta.getIncField());
				js.setDataFormat(meta.getDataFormat());
				js.setLastStartValue(meta.getStartValue());
				//新增时end和start一样，以便后续的更新使用
				js.setLastEndValue(meta.getStartValue());
				dao.insert(js);
				
				setUserVariable("start", meta.getStartValue());
				//新增时始终这是end参数
				setUserVariable("end", endValue);
			}
			setUserVariable("checkColumn", meta.getIncField());
		} catch (Exception e) {
			logBasic("ChinacloudException:SetInitValueFailed," + e.getMessage());
			throw e;
		}
	}
    
	private void setUserVariable(String varname, String value) {
		setVariable(varname, value);

		Trans trans = getTrans();
		trans.setVariable(varname, value);
		while (trans.getParentTrans() != null) {
			trans = trans.getParentTrans();
			trans.setVariable(varname, value);
		}

		// set variale in the root job
		Job parentJob = trans.getParentJob();
		while (parentJob != null) {
			parentJob.setVariable(varname, value);
			parentJob = parentJob.getParentJob();
		}
	}
    

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((IncETLInputMeta) smi);
        this.data = ((IncETLInputData) sdi);
        //设置配置项
    	//this.meta.setJobId(getVariable("jobId"));
    	this.meta.setCheckColumn(getVariable("checkColumn"));
    	this.meta.setEnd(getVariable("end"));
    	this.meta.setUseLastValue(getVariable("useLastValue"));
    	this.meta.setDriverName(getVariable("driver"));
    	this.meta.setMysqlUrl(getVariable("url"));
    	this.meta.setUsername(getVariable("username"));
    	this.meta.setPassword(getVariable("password"));
    	this.meta.setTable(getVariable("table"));
    	this.meta.setUpdateField(getVariable("updateField"));
    	
		logBasic("====设置初始值...");
		Job parentJob = getTrans().getParentJob();
		while (parentJob != null) {
			jobName = parentJob.getJobname();
			parentJob = parentJob.getParentJob();
		}
		
		logBasic("jobName==" + jobName);
		try {
			jobId = MD5.getHashString(Const.getIPAddress()+jobName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.meta.setJobId(jobId);
		setUserVariable("jobId", jobId);
		try {
			setInitValue();
		} catch (Exception e) {
			logError("ChinacloudException:SetInitValueFailed," + e.getMessage());
		}
    	
        return super.init(smi, sdi);
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((IncETLInputMeta) smi);
        this.data = ((IncETLInputData) sdi);
        super.dispose(smi, sdi);
    }

}