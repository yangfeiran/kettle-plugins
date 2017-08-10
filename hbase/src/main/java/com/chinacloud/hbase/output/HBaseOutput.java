package com.chinacloud.hbase.output;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.nutz.dao.Dao;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.chinacloud.hbase.DaoFactory;
import com.chinacloud.hbase.HbaseUtil;
import com.chinacloud.hbase.JobSchedule;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;


public class HBaseOutput extends BaseStep implements StepInterface {
	
	private HBaseOutputData data;
    private HBaseOutputMeta meta;
    
    private AtomicInteger count = new AtomicInteger(0);
    private int rowCount=0;
    
    private String lastEndValue = "";
    
    private HbaseUtil hbaseUtil;
    private Map<String, Map<String,Object>> dataMap = Maps.newHashMap();
    

    public HBaseOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((HBaseOutputMeta) smi);
        this.data = ((HBaseOutputData) sdi);
        Object[] row = getRow();
        RowMetaInterface rowMeta = getInputRowMeta();
        
        if(rowMeta==null){
            setOutputDone();
            return false;
        }
        String[] fields = rowMeta.getFieldNames();
        
        if (row == null) {//没有数据需要处理了
        	boolean result = hbaseUtil.addData(meta.getTableName(), meta.getFamilyName(), dataMap);
			int successCount = rowCount-dataMap.size();
			dataMap.clear();
            //失败则停止该步骤
            if (!result) {
            	logBasic("write data to HBase failed, stop the step!");
				logBasic("SuccessCount:"+successCount);
				if(successCount == 0){
					throw new KettleException("write data to HBase failed! please check the error info.");
				}
				stopAll();
				rowCount = 0;
				return false;
			}else{
				logBasic("SuccessCount:" + rowCount);
	            count.set(0);
			}

			//更新增量字段的值
			lastEndValue = lastEndValue.replaceAll("00Z", "000");
			if(!"true".equals(meta.getUseLastValue()) && !Strings.isNullOrEmpty(meta.getEnd())){
				lastEndValue = meta.getEnd();
				logBasic("use the end as lastEndValue value!");
			}
			if (!Strings.isNullOrEmpty(meta.getJobId())) {
				updateJobInfo(meta.getJobId(), lastEndValue);
			}
        	
        	setOutputDone();
            return false;
        }  
        
        if(count.intValue() == Integer.parseInt(meta.getBatchSize())){
			boolean result = hbaseUtil.addData(meta.getTableName(), meta.getFamilyName(), dataMap);
			int successCount = rowCount-dataMap.size();
			dataMap.clear();
            //失败则停止该步骤
            if (!result) {
            	logBasic("write data to HBase failed, stop the step!");
				logBasic("SuccessCount:"+successCount);
				if(successCount == 0){
					throw new KettleException("write data to HBase failed! please check the error info.");
				}
				stopAll();
				rowCount = 0;
				return false;
			}else{
	            count.set(0);
			}
        }
		
        if (this.first) {
            first = false;
            this.data.outputRowMeta = getInputRowMeta().clone();
            this.meta.getFields(this.data.outputRowMeta, getStepname(), 
            		null, null, this, this.repository, this.metaStore);
        }
        
        Map<String, Object> omap = new HashMap<String, Object>();  
        for(int i = 0; i < row.length; i++){
            if(row[i] != null){
                Object value=row[i];
                omap.put(fields[i],value);
            }
        }  
        if(!omap.containsKey("rowKey")){
        	throw new KettleException("the data rows don't contains field [rowKey]!");
        }
        String rowkey = omap.get("rowKey").toString();
        omap.remove("rowKey");
        dataMap.put(rowkey, omap);
        
        String endValue = String.valueOf(omap.get(meta.getCheckColumn()));
        if(endValue!=null){
        	if(endValue.indexOf("-")!=-1 && endValue.compareTo(lastEndValue) > 0){
				lastEndValue = endValue;
			}else{
				try {
					if(Long.parseLong(endValue) > Long.parseLong(lastEndValue))
						lastEndValue = endValue;
				} catch (Exception e) {
					;
				}
			}
        }
        
        count.incrementAndGet();
        //统计行数
        rowCount++;
        
        putRow(this.data.outputRowMeta, row);
        return true;
    }


	/**
	 * 更新job表信息，增量抽取时使用
	 * @param jobId
	 * @param lastEndValue
	 */
	private void updateJobInfo(String jobId, String lastEndValue) {
		try {
			DaoFactory.init(meta.getDriverName(), meta.getMysqlUrl(),meta.getUsername(), meta.getPassword());
			Dao dao = DaoFactory.getDao();
			JobSchedule js = dao.fetch(JobSchedule.class, jobId);
			if(js == null){
				logBasic("没有设置增量类型，将根据增量字段值自动设置...");
				js = new JobSchedule();
				js.setJobId(jobId);
				boolean isLastModify = lastEndValue.indexOf("-")!=-1;
				js.setIncType(isLastModify ? "lastmodify" : "append");
				js.setDataFormat("yyyy-MM-dd HH:mm:ss");
				js.setLastStartValue(isLastModify ? "1970-01-01 00:00:00" : "0");
				js.setLastEndValue(lastEndValue);
				js.setLastRunTime(new Timestamp(new Date().getTime()));
				dao.insert(js);
			}else{
				if("lastmodify".equals(js.getIncType())){
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
				
				js.setLastStartValue(js.getLastEndValue());
				js.setLastEndValue(lastEndValue);
				js.setLastRunTime(new Timestamp(new Date().getTime()));
				dao.update(js);
			}
		} catch (Exception e) {
			logBasic("ChinacloudException:UpdateJobInfoFailed,"+e.getMessage());
		}
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi){
        this.meta = ((HBaseOutputMeta) smi);
        this.data = ((HBaseOutputData) sdi);
        //设置配置项
    	this.meta.setJobId(getVariable("jobId"));
    	this.meta.setCheckColumn(getVariable("checkColumn"));
    	this.meta.setEnd(getVariable("end"));
    	this.meta.setUseLastValue(getVariable("useLastValue"));
    	this.meta.setDriverName(getVariable("driver"));
    	this.meta.setMysqlUrl(getVariable("url"));
    	this.meta.setUsername(getVariable("username"));
    	this.meta.setPassword(getVariable("password"));
    	this.meta.setTable(getVariable("table"));
    	this.meta.setUpdateField(getVariable("updateField"));
    	this.meta.setDateFields(getVariable("dateFields"));
    	
    	String[] urls = meta.getZkHostPorts().split(";");
    	String zkHosts = "";
    	String zkPort = "2181";
    	for (int i=0; i<urls.length; i++) {
    		String[] ip_port = urls[i].split(":");
			zkHosts += ","+ip_port[0];
        	zkPort = ip_port[1];
		}
    	zkHosts = zkHosts.substring(1);
    	hbaseUtil = new HbaseUtil(meta.getHbaseRootdir(),meta.getHbaseMaster(),zkHosts, zkPort);
    	String[] families = new String[]{meta.getFamilyName()};
		hbaseUtil.createTable(meta.getTableName(), families );
    	
        return super.init(smi, sdi);
    }

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((HBaseOutputMeta) smi);
        this.data = ((HBaseOutputData) sdi);
        hbaseUtil.close();
        
        super.dispose(smi, sdi);
    }

}