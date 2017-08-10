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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.createfile.JobEntryCreateFile;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.chinacloud.incetl.Constant;


public class JobEntryIncETLDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntryCreateFile.class; // for i18n purposes, needed by Translator2!!

  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private Label wlIncType;
  private CCombo wIncType;
  private FormData fdlIncType, fdIncType;
  
  private Label wlIncField;
  private Text wIncField;
  private FormData fdlIncField, fdIncField;
  
  private Label wlDataFormat;
  private CCombo wDataFormat;
  private FormData fdlDataFormat, fdDataFormat;

  private Label wlStartValue;
  private Text wStartValue;
  private FormData fdlStartValue, fdStartValue;
  
  private Label wlEndValue;
  private Text wEndValue;
  private FormData fdlEndValue, fdEndValue;

  private Button wOK, wCancel;
  private Listener lsOK, lsCancel;

  private JobEntryIncETL jobEntry;
  private Shell shell;

  private SelectionAdapter lsDef;

  private boolean changed;

  public JobEntryIncETLDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntryIncETL) jobEntryInt;
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName("作业增量信息配置");
    }
  }

  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    changed = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText("作业增量信息配置");

    int middle = props.getMiddlePct();
    middle -= 10;
    int margin = 6;

    // Filename line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText("作业名称");
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );
    
    //For outputTypeLabel
    this.wlIncType = new Label(this.shell, 131072);
    this.wlIncType.setText("增量类型");
    this.props.setLook(this.wlIncType);
    this.fdlIncType = new FormData();
    this.fdlIncType.left = new FormAttachment(0, 0);
    this.fdlIncType.right = new FormAttachment(middle, -margin);
    this.fdlIncType.top = new FormAttachment(this.wName, margin);
    this.wlIncType.setLayoutData(this.fdlIncType);

    this.wIncType = new CCombo(this.shell, SWT.READ_ONLY); //定义一个只读的下拉框
    for(String val : Constant.incType.keySet()){
    	this.wIncType.add(val);
    }
    this.wIncType.select(0); 
    this.props.setLook(this.wIncType);
    this.wIncType.addModifyListener(lsMod);
    this.fdIncType = new FormData();
    this.fdIncType.left = new FormAttachment(middle, 0);
    this.fdIncType.right = new FormAttachment(100, 0);
    this.fdIncType.top = new FormAttachment(this.wName, margin);
    this.wIncType.setLayoutData(this.fdIncType);
    this.wIncType.addModifyListener(new ModifyListener() {
    	public void modifyText( ModifyEvent e ) {
    		jobEntry.setChanged();
            enableDataFormat();
        }
	});

    //For IncField
    this.wlIncField = new Label(this.shell, 131072);
    this.wlIncField.setText("增量字段");
    this.props.setLook(this.wlIncField);
    this.fdlIncField = new FormData();
    this.fdlIncField.left = new FormAttachment(0, 0);
    this.fdlIncField.right = new FormAttachment(middle, -margin);
    this.fdlIncField.top = new FormAttachment(this.wlIncType, 8);//距上一个控件的位置
    this.wlIncField.setLayoutData(this.fdlIncField);

    this.wIncField = new Text(this.shell, 18436);
    this.props.setLook(this.wIncField);
    this.wIncField.setToolTipText("表中用于增量抽取的字段");
    this.wIncField.addModifyListener(lsMod);
    this.fdIncField = new FormData();
    this.fdIncField.left = new FormAttachment(middle, 0);
    this.fdIncField.right = new FormAttachment(100, 0);
    this.fdIncField.top = new FormAttachment(this.wlIncType, 8);//距上一个控件的位置
    this.wIncField.setLayoutData(this.fdIncField);
    
    //DataFormat
    this.wlDataFormat = new Label(this.shell, SWT.RIGHT);
    this.wlDataFormat.setText("时间戳格式");
    this.props.setLook(this.wlDataFormat);
    this.fdlDataFormat = new FormData();
    this.fdlDataFormat.left = new FormAttachment(0, 0);
    this.fdlDataFormat.right = new FormAttachment(middle, -margin);
    this.fdlDataFormat.top = new FormAttachment(this.wIncField, margin);
    this.wlDataFormat.setLayoutData(this.fdlDataFormat);
    
    this.wDataFormat = new CCombo(this.shell, SWT.BORDER | SWT.READ_ONLY); 
    this.wDataFormat.setEditable(true);
    String[] dats = Const.getDateFormats();
    for ( int x = 0; x < dats.length; x++ ) {
    	this.wDataFormat.add(dats[x]);
    }
    this.props.setLook(this.wDataFormat);
    this.wDataFormat.addModifyListener(lsMod);
    this.fdDataFormat = new FormData();
    this.fdDataFormat.left = new FormAttachment(middle, 0);
    this.fdDataFormat.right = new FormAttachment(100, 0);
    this.fdDataFormat.top = new FormAttachment(this.wIncField, margin);
    this.wDataFormat.setLayoutData(this.fdDataFormat);
    
    //For StartValue
    this.wlStartValue = new Label(this.shell, 131072);
    this.wlStartValue.setText("开始值(必填)");
    this.props.setLook(this.wlStartValue);
    this.fdlStartValue = new FormData();
    this.fdlStartValue.left = new FormAttachment(0, 0);
    this.fdlStartValue.right = new FormAttachment(middle, -margin);
    this.fdlStartValue.top = new FormAttachment(this.wDataFormat, margin);//距上一个控件的位置
    this.wlStartValue.setLayoutData(this.fdlStartValue);
    
    this.wStartValue = new Text(this.shell, 18436);
    this.props.setLook(this.wStartValue);
    this.wStartValue.addModifyListener(lsMod);
    this.fdStartValue = new FormData();
    this.fdStartValue.left = new FormAttachment(middle, 0);
    this.fdStartValue.right = new FormAttachment(100, 0);
    this.fdStartValue.top = new FormAttachment(this.wDataFormat, margin);//距上一个控件的位置
    this.wStartValue.setLayoutData(this.fdStartValue);
    
    //For EndValue
    this.wlEndValue = new Label(this.shell, 131072);
    this.wlEndValue.setText("结束值(可选)");
    this.props.setLook(this.wlEndValue);
    this.fdlEndValue = new FormData();
    this.fdlEndValue.left = new FormAttachment(0, 0);
    this.fdlEndValue.right = new FormAttachment(middle, -margin);
    this.fdlEndValue.top = new FormAttachment(this.wlStartValue, margin);//距上一个控件的位置
    this.wlEndValue.setLayoutData(this.fdlEndValue);
    
    this.wEndValue = new Text(this.shell, 18436);
    this.props.setLook(this.wEndValue);
    this.wEndValue.addModifyListener(lsMod);
    this.fdEndValue = new FormData();
    this.fdEndValue.left = new FormAttachment(middle, 0);
    this.fdEndValue.right = new FormAttachment(100, 0);
    this.fdEndValue.top = new FormAttachment(this.wlStartValue, margin);//距上一个控件的位置
    this.wEndValue.setLayoutData(this.fdEndValue);
    

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wEndValue );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }
  
  protected void enableDataFormat() {
	  boolean flag = "lastmodify".equals(Constant.incType.get(wIncType.getText()));
	  wlDataFormat.setEnabled(flag);
	  wDataFormat.setEnabled(flag);
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( jobEntry.getName() != null ) {
      wName.setText( jobEntry.getName() );
    }
    wIncType.setText(jobEntry.getIncType());
    wIncField.setText(jobEntry.getIncField());
    wDataFormat.setText(jobEntry.getDataFormat());
    wStartValue.setText(jobEntry.getStartValue());
    wEndValue.setText(jobEntry.getEndValue());

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( changed );
    jobEntry = null;
    dispose();
  }

  private void ok() {

    if ( Const.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setIncType(wIncType.getText());
    jobEntry.setIncField(wIncField.getText());
    jobEntry.setDataFormat(wDataFormat.getText());
    jobEntry.setStartValue(wStartValue.getText());
    jobEntry.setEndValue(wEndValue.getText());
    dispose();
  }

  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return false;
  }
}
