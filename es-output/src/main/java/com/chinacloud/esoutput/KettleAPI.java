package com.chinacloud.esoutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.metadata.datatable.Row;

import com.google.common.collect.Lists;

public class KettleAPI {

	public static void main(String[] args) {
		try {
			KettleEnvironment.init();
			KettleDatabaseRepository repository = new KettleDatabaseRepository();
			DatabaseMeta dataMeta = new DatabaseMeta("Kettle_MySQL", "Mysql", 
					"jdbc", "172.16.50.80", "magicwand", "3306", "root", "hadoop");
			KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta(
					"Kettle", "Kettle", "king description", dataMeta);
			repository.init(kettleDatabaseMeta);
			repository.connect("admin", "admin");
			RepositoryDirectoryInterface directory = repository.findDirectory("/");
			String transName = "abcd.ktr";
			String excuteType = transName.substring(transName.lastIndexOf(".") + 1, transName.length());
			transName = transName.substring(0, transName.lastIndexOf("."));

			if (excuteType.equals("job")) {
				JobMeta meta = ((Repository) repository).loadJob(transName, directory, null, null);
				Job job = new Job(repository, meta);
				job.run();
				job.waitUntilFinished();
				if (job.getErrors() > 0) {
					System.err.println(excuteType + ":Transformation run Failure!");
				} else {
					System.out.println(excuteType + ":Transformation run successfully!");
				}
			} else {
				TransMeta transMeta = ((Repository) repository)
						.loadTransformation(transName, directory, null, true, null);
				Trans trans = new Trans(transMeta);
				trans.prepareExecution(null);
				String stepName = "字段选择";
				StepInterface step = trans.findRunThread(stepName);

				final List<Map<String, Object>> list = Lists.newArrayList();
				step.addRowListener(new RowAdapter() {
					public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
						// Here you get the rows as they are read by the step

					}

					public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
						// Here you get the rows as they are written by the step
						String[] fields = rowMeta.getFieldNames();
						Map<String, Object> omap = new HashMap<String, Object>();
						for (int i = 0; i < row.length; i++) {
							if (row[i] != null) {
								Object value = row[i];
								omap.put(fields[i], value);
							}
						}
						list.add(omap);
					}
				});

				trans.startThreads();
				trans.waitUntilFinished();
				Result result = trans.getResult();
				for (Map<String, Object> row : list) {
					System.out.println(row);
				}
				if (trans.getErrors() > 0) {
					System.err.println(excuteType + ":Transformation run Failure!");
				} else {
					System.out.println(excuteType + ":Transformation run successfully!");
				}
			}
		} catch (KettleException e) {
			e.printStackTrace();
		}
	}

}
