package com.chinacloud.esoutput;

import java.util.Date;
import java.sql.*; 
import java.io.*; 

import org.nutz.dao.entity.annotation.*;

@Table("job_schedule")
public class JobSchedule { 
	@Name
	private String jobId;
	@Column
	private String jobName;
	@Column
	private String incType;
	@Column
	private String incField;
	@Column
	private String dataFormat;
	@Column
	private String lastStartValue;
	@Column
	private String lastEndValue;
	@Column
	private Timestamp lastRunTime;

	public void setJobId(String jobid){
		this.jobId = jobid;
	}
	public String getJobId(){
		return this.jobId;
	}

	public void setJobName(String jobname){
		this.jobName = jobname;
	}
	public String getJobName(){
		return this.jobName;
	}

	public void setIncType(String inctype){
		this.incType = inctype;
	}
	public String getIncType(){
		return this.incType;
	}

	public void setIncField(String incfield){
		this.incField = incfield;
	}
	public String getIncField(){
		return this.incField;
	}

	public void setDataFormat(String dataformat){
		this.dataFormat = dataformat;
	}
	public String getDataFormat(){
		return this.dataFormat;
	}

	public void setLastStartValue(String laststartvalue){
		this.lastStartValue = laststartvalue;
	}
	public String getLastStartValue(){
		return this.lastStartValue;
	}

	public void setLastEndValue(String lastendvalue){
		this.lastEndValue = lastendvalue;
	}
	public String getLastEndValue(){
		return this.lastEndValue;
	}

	public void setLastRunTime(Timestamp lastruntime){
		this.lastRunTime = lastruntime;
	}
	public Timestamp getLastRunTime(){
		return this.lastRunTime;
	}
}
