package com.chinacloud.esoutput;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
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

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class EDBOutput extends BaseStep implements StepInterface {
    private EDBOutData data;
    private EDBOutMeta meta;
    private AtomicInteger count = new AtomicInteger(0);
    private List<Map<String,Object>> olist = new ArrayList<Map<String, Object>>();
    private Map<String, List<Map<String,Object>>> childMap = Maps.newHashMap();
    private String jobId ;
    private String checkColumn ;
    private String updateAtLast ;
    private String lastEndValue="0";
    private int rowCount=0;
    private String pId = null;
    
    private int batchSize = 5000;
    
    private int timeOutMin = 10;
    private SQL4ESUtil sql4esUtil = null;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    

    public EDBOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
        Object[] row = getRow();
        RowMetaInterface rowMeta = getInputRowMeta();
        
        if(rowMeta==null){
            setOutputDone();
            olist.clear();
            return false;
        }
        String[] fields = rowMeta.getFieldNames();   
        
        if (row == null) {//没有数据需要处理了
        	boolean result = false;
        	int successCount = getSuccessCount();
			if("inner-update".equals(meta.getOutputType())){
				result = loadDatas(childMap);
				childMap.clear();
			}else{
				result = loadDatas(olist); 
				olist.clear();
			}
			//更新增量字段的值
			if(result && !Strings.isNullOrEmpty(jobId)){
				updateLastValue(lastEndValue);
			}
            //失败则停止该步骤
            if (!result) {
            	logBasic("post data to EDB failed, stop the step!");
            	
				logBasic("SuccessCount:"+successCount);
				if(successCount == 0){
					throw new KettleException("post data to EDB failed! please check the error info.");
				}
            	stopAll();
            }else{
            	logBasic("SuccessCount:" + rowCount);
            	setOutputDone();
            }
            rowCount = 0;

            return false;
        } 
        
		if(count.intValue() == batchSize){
			int successCount = getSuccessCount();
			boolean result = false;
			if("inner-update".equals(meta.getOutputType())){
				result = loadDatas(childMap);
				childMap.clear();
			}else{
				result = loadDatas(olist); 
				olist.clear();
			}
			//是否每批次提交后都更新最大值
			if(result && !"true".equals(updateAtLast) && !Strings.isNullOrEmpty(jobId)){
				updateLastValue(lastEndValue);
			}
			
            //失败则停止该步骤
            if (!result) {
            	logBasic("post data to EDB failed, stop the step!");
            	if(!"true".equals(updateAtLast)){
            		logBasic("SuccessCount:"+successCount);
            	}
				if("true".equals(updateAtLast) || successCount == 0){
					throw new KettleException("post data to EDB failed! please check the error info.");
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
            this.meta.getFields(this.data.outputRowMeta, getStepname(), null, null, this);
        }
        

        Map<String, Object> omap = new HashMap<String, Object>();  
        for(int i = 0; i < fields.length; i++){
            if(row[i]!=null || "parentId".equals(fields[i])){
            	omap.put(fields[i],row[i]);
            }
        }  
  
        if("inner-update".equals(meta.getOutputType())){
        	if(omap.containsKey("parentId")){
        		Object parentVal = omap.get("parentId");
        		if(parentVal!=null){
        			pId = parentVal.toString();
        		}else{
        			pId = null;
        		}
        		omap.remove("parentId");
            }else{
        		throw new KettleException("the data rows don't contains field [parentId]!");
        	}
        	if(!omap.containsKey("_rowid")){
        		throw new KettleException("the data rows don't contains field [_rowid]!");
        	}
        	if(childMap.containsKey(pId)){
        		childMap.get(pId).add(omap);
        	}else if(!Strings.isNullOrEmpty(pId)){
        		List<Map<String,Object>> childList = Lists.newArrayList();
        		childList.add(omap);
        		childMap.put(pId, childList);
        	}
        	count.set(childMap.size());
        }else{
        	olist.add(omap);
        	count.incrementAndGet();
        }  

        String endValue = String.valueOf(omap.get(checkColumn));
        if(endValue!=null && (!"update".equals(meta.getOutputType()) || !Strings.isNullOrEmpty(pId))){
        	if(endValue.indexOf("-")!=-1){
        		if(endValue.compareTo(lastEndValue) > 0)
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
        
        //统计行数
        if(!"inner-update".equals(meta.getOutputType()) || !Strings.isNullOrEmpty(pId)){
        	rowCount++;
        }
        if ( checkFeedback( getLinesRead() ) ) {
        	logBasic("已处理:"+getLinesRead());
        }
        putRow(this.data.outputRowMeta, row);
        return true;
    }
   
    /**
	 * @return
	 */
	private int getSuccessCount() {
		if("inner-update".equals(meta.getOutputType())){
			int n = 0;
			for (List<Map<String, Object>> list : childMap.values()) {
				n += list.size();
			}
			return rowCount - n;
		}else{
			return rowCount-olist.size();
		}
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
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
    	jobId =  getVariable("jobId");
        checkColumn =  getVariable("checkColumn");
        updateAtLast = getVariable("updateAtLast");
        if(!Strings.isNullOrEmpty(meta.getBatchSize())){
        	batchSize = Integer.parseInt(meta.getBatchSize());
        }
        if(!Strings.isNullOrEmpty(getVariable("timeOutMin"))){
        	try {
        		timeOutMin = Integer.parseInt(getVariable("timeOutMin"));
			} catch (Exception e) {
				;
			}
        }
        
		try {
			InetAddress netAddress = InetAddress.getLocalHost();
			logBasic("*****Running at: "+netAddress.getHostAddress()+"*****");
		} catch (UnknownHostException e) {
			;
		}
        logBasic("==jobId=="+jobId);
        
    	//
    	String[] info = meta.getUrl().split(";");
		String[] ips = new String[info.length];
		int[] ports = new int[info.length];
		for(int i=0;i<info.length;i++){
			ips[i] = info[i].split("[:]")[0];
			ports[i] = Integer.parseInt(info[i].split("[:]")[1]);
		}
		String clusterName = meta.getClusterName(); // 集群名称
		sql4esUtil = new SQL4ESUtil(clusterName, ips, ports);
    	
        return super.init(smi, sdi);
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((EDBOutMeta) smi);
        this.data = ((EDBOutData) sdi);
        //最后关闭es连接
        logBasic("close es connection...");
        sql4esUtil.close();
        
        executorService.shutdownNow();
        
        super.dispose(smi, sdi);
    }
    
    public boolean loadDatas(List<Map<String, Object>> olist) {
		if (olist.size() == 0) {
			return true;
		}
		
		String result = doPostESql(olist);
		// 再次尝试
		if (!"OK".equals(result)) {
			File file = new File("errorData-"+jobId+".txt");
			logError("!!!post data to EDB error,write data to file["+file.getAbsolutePath()+"]...");
			try {
				FileUtils.writeStringToFile(file, JSON.toJSONString(childMap), "utf8");
			} catch (IOException e) {
				e.printStackTrace();
			}
			logBasic("post data to EDB second time...");
			sql4esUtil.close();
			result = doPostESql(olist);
			// 再次尝试
			if (!"OK".equals(result)) {
				logBasic("post data to EDB third time...");
				sql4esUtil.close();
				result = doPostESql(olist);
			}
		}

		return "OK".equals(result);
	}
	
	/**
	 * @param childMap
	 * @return
	 */
	public boolean loadDatas(Map<String, List<Map<String,Object>>> childMap) {
		if (childMap.size() == 0) {
			return true;
		}
		
		String result = doPostESql(childMap);
		// 再次尝试
		if (!"OK".equals(result)) {
			File file = new File("errorData-"+jobId+".txt");
			logError("!!!post data to EDB error,write data to file["+file.getAbsolutePath()+"]...");
			try {
				FileUtils.writeStringToFile(file, JSON.toJSONString(childMap), "utf8");
			} catch (IOException e) {
				e.printStackTrace();
			}
			logBasic("post data to EDB second time...");
			sql4esUtil.close();
			result = doPostESql(childMap);
			// 再次尝试
			if (!"OK".equals(result)) {
				logBasic("post data to EDB third time...");
				sql4esUtil.close();
				result = doPostESql(childMap);
			}
		}

		return "OK".equals(result);
	}

	public void updateLastValue(String lastEndValue) {
		try {
			lastEndValue = lastEndValue.replaceAll("000Z", "000");
			//使用end参数作为增量结束值
			if(!"true".equals(meta.getUseLastValue()) && !Strings.isNullOrEmpty(meta.getEnd())){
				lastEndValue = meta.getEnd();
				//logBasic("use the end as lastModified value!");
			}
			updateJobInfo(lastEndValue);
		} catch (Exception e) {
			logBasic("update jobinfo error..."+e.getMessage());
			e.printStackTrace();
		}
	}
	
	public String doPostESql(final List<Map<String, Object>> olist) {
		String result = "Unknown Error";
		//result = sql4esUtil.bulkRequest(database, outputTable, olist);
		//可能存在修改旧数据，使用更新的方式增加数据
		if(sql4esUtil.disconnected()){
			sql4esUtil.open();
		}
		List<Callable<String>> tasks = Lists.newArrayList();
		tasks.add(new Callable<String>() {
			@Override
			public String call() throws Exception {
				if("add-only".equals(meta.getOutputType())){
					return sql4esUtil.bulkRequest(meta.getDatabase(), meta.getOutputTable(), 
							new ArrayList<Map<String, Object>>(olist));
				}else{
					return sql4esUtil.bulkAdd(meta.getDatabase(), meta.getOutputTable(), 
							new ArrayList<Map<String, Object>>(olist));
				}
				
			}
		});
		try {
			List<Future<String>> futures = executorService.invokeAll(tasks, timeOutMin, TimeUnit.MINUTES);
			for (Future<String> future : futures) {
				result = future.get();
			}
		} catch (Exception e) {
			logError("==="+e.toString());
			logError("!!!"+e.getMessage());
			result = e.getMessage();
		}
		sql4esUtil.close();
		
		if (!"OK".equals(result)) {
			logBasic("ChinacloudException:" + result);
		}
		return result;
	}
	
	/**
	 * 针对内嵌表的更新
	 * @param map
	 * @return
	 */
	public String doPostESql(final Map<String, List<Map<String,Object>>> map) {
		String result = "Unknown Error";
		if(sql4esUtil.disconnected()){
			sql4esUtil.open();
		}
		List<Callable<String>> tasks = Lists.newArrayList();
		tasks.add(new Callable<String>() {
			@Override
			public String call() throws Exception {
				return sql4esUtil.bulkUpdate(meta.getDatabase(), meta.getOutputTable(), 
						meta.getFieldName(), meta.getStatFieldName(), meta.getStatCnd(),
						new HashMap<String, List<Map<String,Object>>>(map));
			}
		});
		try {
			List<Future<String>> futures = executorService.invokeAll(tasks, timeOutMin, TimeUnit.MINUTES);
			for (Future<String> future : futures) {
				result = future.get();
			}
		} catch (Exception e) {
			logError("==="+e.toString());
			logError("!!!"+e.getMessage());
			result = e.getMessage();
		}
		sql4esUtil.close();
		
		if (!"OK".equals(result)) {
			logBasic("ChinacloudException:" + result);
		}
		return result;
	}
	
	/**
	 * 更新job信息
	 * 
	 * @param lastEndValue
	 * @return
	 */
	private boolean updateJobInfo(String lastEndValue) {
		try {
			DaoFactory.init(meta.getDriverName(), meta.getMysqlUrl(), meta.getUsername(), meta.getPassword());
			Dao dao = DaoFactory.getDao();
			JobSchedule js = dao.fetch(JobSchedule.class, jobId);
			if(lastEndValue.length()>23){
				lastEndValue = lastEndValue.substring(0, 23);
			}
			lastEndValue = lastEndValue.replaceAll("T", " ");
			if(js == null){
				logBasic("没有设置增量类型，将根据增量字段值自动设置...");
				js = new JobSchedule();
				js.setJobId(jobId);
				boolean isLastModify = lastEndValue.indexOf("-")!=-1;
				js.setIncType(isLastModify ? "lastmodify" : "append");
				js.setDataFormat(isLastModify ? "yyyy-MM-dd HH:mm:ss" : "");
				js.setLastStartValue(isLastModify ? "1970-01-01 00:00:00" : "0");
				js.setLastEndValue(lastEndValue);
				js.setIncField(meta.getCheckColumn());
				js.setLastRunTime(new Timestamp(new Date().getTime()));
				dao.insert(js);
			}else{
				if("lastmodify".equals(js.getIncType())){
					//根据实际数据生成时间格式字符串
					String df = js.getDataFormat().replaceAll("'T'", " ");
					StringBuffer value = new StringBuffer(lastEndValue);
					value.setCharAt(4, df.charAt(4));
					value.setCharAt(7, df.charAt(7));
					lastEndValue = value.toString();
				}
				
				js.setLastStartValue(js.getLastEndValue());
				js.setLastEndValue(lastEndValue);
				js.setLastRunTime(new Timestamp(new Date().getTime()));
				dao.update(js);
			}
			
			return true;
		} catch (Exception e) {
			logBasic("ChinacloudException:UpdateJobInfoFailed,"+e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	

}