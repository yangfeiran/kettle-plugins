package com.chinacloud.orcparquet;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.nutz.dao.Dao;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.io.IOException;
import java.sql.*;
import java.util.Date;


public class OrcParquetOutput extends BaseStep implements StepInterface {
	
	private OrcParquetOutData data;
    private OrcParquetOutMeta meta;
    private IProxy writeProxy;
    private int rowCount=0;
    private String lastEndValue = "0";
    private String fileName;
    private String sql = "";
    
    public OrcParquetOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        this.meta = ((OrcParquetOutMeta) smi);
        this.data = ((OrcParquetOutData) sdi);
        Object[] row = getRow();
        RowMetaInterface rowMeta = getInputRowMeta();
        
        if(rowMeta==null){
            setOutputDone();
            return false;
        }
        String[] fields = rowMeta.getFieldNames();
        
        if (row == null) {//没有数据需要处理了
        	try {
				writeProxy.close();
				//更新增量字段的值
				lastEndValue = lastEndValue.replaceAll("000Z", "000");
				if (!Strings.isNullOrEmpty(meta.getJobId())) {
					if(!Strings.isNullOrEmpty(meta.getEnd())){//有end参数，表示按时间或者分段抽取
						if(!"true".equals(meta.getUseLastValue())){
							lastEndValue = meta.getEnd();
							logBasic("use the end as lastEndValue value!");
						}
					}
					logBasic("**update lastEndValue: "+lastEndValue);
					updateJobInfo(meta.getJobId(), lastEndValue);
				}
				//是否创建hive表并导入数据
				if(meta.isCreateAndLoad()){
					if(createTableAndLoadData()){
						logBasic("SuccessCount:" + rowCount);
					}
				}
			} catch (Exception e) {
				logBasic("write parquet file to HDFS failed, stop the step!");
				logBasic("SuccessCount:0");
				e.printStackTrace();
				throw new KettleStepException(e.getCause());
			}
        	
        	setOutputDone();
            return false;
        }  
		
        if (this.first) {
            first = false;
            this.data.outputRowMeta = getInputRowMeta().clone();
            this.meta.getFields(this.data.outputRowMeta, getStepname(), 
            		null, null, this, this.repository, this.metaStore);
            //获取HDFS节点信息
            String hdfsUrls = environmentSubstitute(meta.getUrl());
            fileName = environmentSubstitute(meta.getFileName());
            if(Const.isEmpty(fileName)){
            	throw new KettleException("输出文件名为空，请正确设置文件名！");
            }
            Configuration conf = getConf(hdfsUrls);
            try {
	            if("parquet".equals(this.meta.getOutputType())){
	            	MessageType schema = getParquetShema(rowMeta, fields);
	                SimpleGroupFactory f = new SimpleGroupFactory(schema);
	                writeProxy = new ParquetProxy(conf, fileName, schema, f, meta.isCleanOutput());
	            }else if("orc".equals(this.meta.getOutputType())){
	            	TypeDescription schema = getOrcSchema(rowMeta, fields);
	            	writeProxy = new OrcProxy(conf, fileName, schema, meta.isCleanOutput());
	            }
	            writeProxy.setBlockSize(Integer.parseInt(environmentSubstitute(meta.getBlockSize()))*1024*1024);
                writeProxy.setPageSize(Integer.parseInt(environmentSubstitute(meta.getPageSize()))*1024);
                writeProxy.start();
    		} catch (KettleFileException | IOException e) {
    			logBasic(e.getMessage());
    			throw new KettleStepException(e.getCause());
    		}
        }
        //写入一行数据
        try {
        	writeProxy.writeRow(row, rowMeta, fields);
		} catch (IOException e) {
			logBasic(e.getMessage());
			throw new KettleStepException("Error writing row", e);
		}
        
        //选择排序获取最后更新值
        String endValue = null;
        for(int i=0;i<fields.length;i++){
        	if(fields[i].equals(meta.getCheckColumn())){
        		try {
        			endValue = (String) row[i];
				} catch (Exception e) {
					endValue = String.valueOf(row[i]);
				}
        		break;
        	}
        }
        if(endValue!=null){
        	if(endValue.indexOf("-")!=-1 && endValue.compareTo(lastEndValue) > 0){
				lastEndValue = endValue;
			}else{
				try {
					if(Long.parseLong(endValue) > Long.parseLong(lastEndValue))
						lastEndValue = endValue;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
        }
        
        if ( checkFeedback( getLinesRead() ) ) {
            logBasic( "已处理：" + getLinesRead() );
        }
        
        //统计行数
        rowCount++;
        
        putRow(this.data.outputRowMeta, row);
        return true;
    }

	private boolean createTableAndLoadData() throws Exception {
		try {
			String driverName = "org.apache.hive.jdbc.HiveDriver";
			if(meta.getHiveVersion().equals("hive")){
				 driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
			}
			Class.forName(driverName);
			/*String url = "jdbc:" + meta.getHiveVersion() + "://" + meta.getHiveHost() + ":"
					+ meta.getHivePort() + "/" + meta.getHiveDB();*/
			String url = meta.getHiveUrl();
			if (meta.getHiveVersion().equals("impala")) {
				if (!url.contains("auth=")) {
					url += ";auth=noSasl";
				}
			}
			logBasic("connect to hive["+url+"] by user "+meta.getHiveUser());
			Connection con = DriverManager.getConnection(url, meta.getHiveUser(), meta.getHivePassword());
			Statement stmt = con.createStatement();
			
			//try to create table, if exist, continue
			String msg = "try to create table: " + meta.getHiveTable() + ", ";
			try {
				stmt.execute(sql);
				msg += "[created true]";
				logBasic(msg + "[created true]");
			} catch (SQLException e) {
				logBasic(msg + "[created false]");
				logBasic("**create table sql::"+sql);
				logError(e.getMessage());
				throw e;
			}
			
			logBasic("load data to hive table: " + meta.getHiveTable()+"...");
			sql = "load data inpath '"+fileName+"' ";
			if(meta.isOverwriteTable()){
				sql += "overwrite ";
			}
			sql += "into table "+meta.getHiveTable();
			try{
				stmt.execute(sql);
			}catch (Exception e) {
				logError("load data error...");
				logBasic("==sql=="+sql);
				throw e;
			}
			logBasic("load data finish!");
			
			if(meta.getExcuteSql()){
				String exeSql = environmentSubstitute(meta.getSqlContent());
				logBasic("===execute sql: " + exeSql);
				try{
					stmt.execute(exeSql);
				}catch (Exception e) {
					logError("execute sql after load data error: " + e.getMessage());
				}
			}
	    	
	    	con.close();
	    	
	    	return true;
		} catch (Exception e) {
			logError(e.getMessage());
			throw e;
		}
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
			if(lastEndValue.length()>23){
				lastEndValue = lastEndValue.substring(0, 23);
			}
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
		} catch (Exception e) {
			logBasic("ChinacloudException:UpdateJobInfoFailed,"+e.getMessage());
		}
	}

	private TypeDescription getOrcSchema(RowMetaInterface rowMeta, String[] fields) {
		sql = "create table if not exists " + meta.getHiveTable() +"(";
		TypeDescription schema = TypeDescription.createStruct();
		for(int i=0;i<fields.length;i++){
			sql += fields[i];
    		switch (rowMeta.getValueMeta(i).getType()){
    	    case 5:
    	    	schema.addField(fields[i], TypeDescription.createLong());
    	    	sql += " bigint, ";
    	    	break;
    	    case 1:
    	    case 6:
    	    	schema.addField(fields[i], TypeDescription.createDouble());
    	    	sql += " double, ";
    	        break;
    	    case 4:
    	    	schema.addField(fields[i], TypeDescription.createBoolean());
    	    	sql += " boolean, ";
    	        break;
    	    case 2:
    	    case 3:
    	    default:
    	    	schema.addField(fields[i], TypeDescription.createString());
    	    	sql += " string, ";
    	    }
    	}
		sql = sql.substring(0, sql.length()-2) + ") stored AS orc";
		return schema;
	}

	/**
	 * 获取kettle数据流各字段的类型
	 * Number==1
	 * String==2
	 * Date==3
	 * Boolean==4
	 * Integer==5
	 * BigNumber==6
	 * Binary==8
	 * Timestamp==9
	 * Internet Address==10
	 * @param rowMeta
	 * @param fields
	 * @return
	 */
	private MessageType getParquetShema(RowMetaInterface rowMeta, String[] fields) {
		sql = "create table if not exists " + meta.getHiveTable() +"(";
    	StringBuffer message = new StringBuffer("message m {");
    	for(int i=0;i<fields.length;i++){
    		sql += fields[i];
    		switch (rowMeta.getValueMeta(i).getType()){
    	    case 5:
    	    	message.append(" required int64 ").append(fields[i]).append(";");
    	    	sql += " bigint, ";
    	        break;
    	    case 1:
    	    case 6:
    	    	message.append(" required double ").append(fields[i]).append(";");
    	    	sql += " double, ";
    	        break;
    	    case 4:
    	    	message.append(" required boolean ").append(fields[i]).append(";");
    	    	sql += " boolean, ";
    	        break;
    	    case 2:
    	    case 3:
    	    default:
    	    	message.append(" required binary ").append(fields[i]).append(" (UTF8);");
    	    	sql += " string, ";
    	    }
    	}
    	message.append(" }");
    	sql = sql.substring(0, sql.length()-2) + ") stored AS parquet";

    	return MessageTypeParser.parseMessageType(message.toString());
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi){
        this.meta = ((OrcParquetOutMeta) smi);
        this.data = ((OrcParquetOutData) sdi);
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
    	
    	if(meta.isCreateAndLoad() && Strings.isNullOrEmpty(meta.getHiveUser())){
    		logBasic("==set the HADOOP_USER_NAME to config value: HiveUser");
    		System.setProperty("HADOOP_USER_NAME", meta.getHiveUser());
    	}else {
    		logBasic("==set the HADOOP_USER_NAME to default value: hive");
    		System.setProperty("HADOOP_USER_NAME", "hive");
		}
    	
        return super.init(smi, sdi);
    }

    /**
     * HDFS集群的master节点的ip:port，多个ip:port之间用英文;分隔
     * @param hdfsUrls 172.16.50.24:8020;172.16.50.21:8020
     * @return
     */
    private Configuration getConf(String hdfsUrls) {
    	String[] urls = hdfsUrls.split(";");
    	String ClusterName = "nss";
    	String HADOOP_URL = "hdfs://"+ClusterName;
    	Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HADOOP_URL);
        conf.set("dfs.nameservices", ClusterName);
        String namenodes = "";
        for (int i=0; i<urls.length; i++) {
        	namenodes += ",nn"+i;
        	conf.set("dfs.namenode.rpc-address."+ClusterName+".nn"+i, urls[i]);
		}
        conf.set("dfs.ha.namenodes."+ClusterName, namenodes.substring(1));
        conf.set("dfs.client.failover.proxy.provider."+ClusterName, 
        		"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		return conf;
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        this.meta = ((OrcParquetOutMeta) smi);
        this.data = ((OrcParquetOutData) sdi);
        super.dispose(smi, sdi);
    }

}