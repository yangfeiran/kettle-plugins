package com.chinacloud.orcparquet;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.tableinput.SQLValuesHighlight;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class OrcParquetOutDialog extends BaseStepDialog
        implements StepDialogInterface {
    private static Class<?> PKG = OrcParquetOutDialog.class;
    private OrcParquetOutMeta input;

    private Label fileNameLabel;
    private TextVar fileName;
    private FormData fileNameFormData, fileNameTxtFormData;
    
    private Label wlCleanOutput;
    private Button wCleanOutput;
    private FormData fdlCleanOutput, fdCleanOutput;
    
    private Label outputTypeLabel;
    private CCombo outputType;
    private FormData outputTypeFormData, outputTypeComFormData;
    
    private Label urlLabel;
    private TextVar url;
    private FormData urlFormData, urlTxtFormData;
    
    private Label blockSizeLabel;
    private TextVar blockSize;
    private FormData blockSizeFormData, blockSizeTxtFormData;
    
    private Label pageSizeLabel;
    private TextVar pageSize;
    private FormData pageSizeFormData, pageSizeTxtFormData;
    
    private Label wlCreateAndLoad;
    private Button wCreateAndLoad;
    private FormData fdlCreateAndLoad, fdCreateAndLoad;
    
    private Label wlHiveVersion;
    private CCombo wHiveVersion;
    private FormData fdlHiveVersion, fdHiveVersion;
    
/*    private Label wlHiveHost;
    private Text wHiveHost;
    private FormData fdlHiveHost, fdHiveHost;
    
    private Label wlHivePort;
    private Text wHivePort;
    private FormData fdlHivePort, fdHivePort;
    
    private Label wlHiveDB;
    private Text wHiveDB;
    private FormData fdlHiveDB, fdHiveDB;*/

    private Label wlHiveUrl;
    private Text wHiveUrl;

    private Label wlHiveUser;
    private Text wHiveUser;
    private FormData fdlHiveUser, fdHiveUser;
    
    private Label wlHivePassword;
    private Text wHivePassword;
    private FormData fdlHivePassword, fdHivePassword;
    
    private Label wlHiveTable;
    private Text wHiveTable;
    private FormData fdlHiveTable, fdHiveTable;
    
    private Label wlOverwriteTable;
    private Button wOverwriteTable;
    private FormData fdlOverwriteTable, fdOverwriteTable;
    
    
    private Label wlExecuteSQL;
    private Button wExecuteSQL;
    private FormData fdlExecuteSQL, fdExecuteSQL;
    
    private Label sqlContentLabel;
    private StyledTextComp sqlContent;
    private FormData sqlContentFormData, sqlContentTxtFormData;

	
    private Button wTestConnect;
	private Group wHiveGroup;
	private FormData fdHiveGroup;
	
	
	public OrcParquetOutDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.input = ((OrcParquetOutMeta) in);
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);
        this.shell.setSize(200, 200);
        this.props.setLook(this.shell);
        
        /*helpComp = new Composite( shell, SWT.NONE );
        helpComp.setLayout( new FormLayout() );
        GridData helpCompData = new GridData();
        helpCompData.grabExcessHorizontalSpace = true;
        helpCompData.grabExcessVerticalSpace = false;
        helpComp.setLayoutData( helpCompData );*/
        
        setShellImage(this.shell, this.input);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                OrcParquetOutDialog.this.input.setChanged();
            }
        };
        this.changed = this.input.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        
        this.shell.setLayout(formLayout);
        this.shell.setText("HDFS输出配置");

        int middle = this.props.getMiddlePct()-10;
        int margin = 6;

        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText(BaseMessages.getString("System.Label.StepName"));
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.right = new FormAttachment(middle, -margin);
        this.fdlStepname.top = new FormAttachment(0, margin);
        this.wlStepname.setLayoutData(this.fdlStepname);

        this.wStepname = new Text(this.shell, 18436);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        this.fdStepname = new FormData();
        this.fdStepname.left = new FormAttachment(middle, 0);
        this.fdStepname.top = new FormAttachment(0, margin);
        this.fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(this.fdStepname);
        
        //For FileName
        this.fileNameLabel = new Label(this.shell, 131072);
        this.fileNameLabel.setText("文件名");
        this.props.setLook(this.fileNameLabel);
        this.fileNameFormData = new FormData();
        this.fileNameFormData.left = new FormAttachment(0, 0);
        this.fileNameFormData.right = new FormAttachment(middle, -margin);
        this.fileNameFormData.top = new FormAttachment(this.wStepname, margin);//距上一个控件的位置
        this.fileNameLabel.setLayoutData(this.fileNameFormData);
        
        this.fileName = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.fileName);
        this.fileName.addModifyListener(lsMod);
        this.fileNameTxtFormData = new FormData();
        this.fileNameTxtFormData.left = new FormAttachment(middle, 0);
        this.fileNameTxtFormData.right = new FormAttachment(100, 0);
        this.fileNameTxtFormData.top = new FormAttachment(this.wStepname, margin);//距上一个控件的位置
        this.fileName.setLayoutData(this.fileNameTxtFormData);
        
        //覆盖原文件？
        this.wlCleanOutput = new Label(shell, SWT.RIGHT );
        this.wlCleanOutput.setText("覆盖输出文件？");
        this.props.setLook(this.wlCleanOutput);
        this.fdlCleanOutput = new FormData();
        this.fdlCleanOutput.left = new FormAttachment(0, 0);
        this.fdlCleanOutput.right = new FormAttachment(middle, -margin);
        this.fdlCleanOutput.top = new FormAttachment(this.fileName, margin);
        this.wlCleanOutput.setLayoutData(this.fdlCleanOutput);

        this.wCleanOutput = new Button(shell, SWT.CHECK);
        this.wCleanOutput.setToolTipText("当输出文件已经存在时，如果勾选该项则会被覆盖，\n否则会抛出文件已存在的异常。");
        this.props.setLook(this.wCleanOutput);
        this.fdCleanOutput = new FormData();
        this.fdCleanOutput.left = new FormAttachment(middle, 0);
        this.fdCleanOutput.top = new FormAttachment(this.fileName, margin);
        this.fdCleanOutput.right = new FormAttachment(100, 0);
        this.wCleanOutput.setLayoutData(this.fdCleanOutput);
        this.wCleanOutput.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
              input.setChanged();
            }
        });
        
        //For outputTypeLabel
        this.outputTypeLabel = new Label(this.shell, 131072);
        this.outputTypeLabel.setText("输出文件类型");
        this.props.setLook(this.outputTypeLabel);
        this.outputTypeFormData = new FormData();
        this.outputTypeFormData.left = new FormAttachment(0, 0);
        this.outputTypeFormData.right = new FormAttachment(middle, -margin);
        this.outputTypeFormData.top = new FormAttachment(this.wCleanOutput, margin);
        this.outputTypeLabel.setLayoutData(this.outputTypeFormData);

        this.outputType = new CCombo(this.shell, SWT.BORDER | SWT.READ_ONLY ); //定义一个只读的下拉框
        this.outputType.add("parquet");
        this.outputType.add("orc");
        //this.outputType.add("txt");
        this.outputType.select(0); 
        this.props.setLook(this.outputType);
        this.outputType.addModifyListener(lsMod);
        this.outputTypeComFormData = new FormData();
        this.outputTypeComFormData.left = new FormAttachment(middle, 0);
        this.outputTypeComFormData.right = new FormAttachment(100, 0);
        this.outputTypeComFormData.top = new FormAttachment(this.wCleanOutput, margin);
        this.outputType.setLayoutData(this.outputTypeComFormData);
        this.outputType.addModifyListener(new ModifyListener() {
        	public void modifyText( ModifyEvent e ) {
                input.setChanged();
                enablePagesize();
            }
		});

        //For Url
        this.urlLabel = new Label(this.shell, 131072);
        this.urlLabel.setText("HDFS地址端口(支持HA)");
        this.urlLabel.setToolTipText("HDFS集群的地址和端口");
        this.props.setLook(this.urlLabel);
        this.urlFormData = new FormData();
        this.urlFormData.left = new FormAttachment(0, 0);
        this.urlFormData.right = new FormAttachment(middle, -margin);
        this.urlFormData.top = new FormAttachment(this.outputTypeLabel, 8);//距上一个控件的位置
        this.urlLabel.setLayoutData(this.urlFormData);

        this.url = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.url);
        this.url.setToolTipText("HDFS集群master节点的地址和端口，多个之间用英文分号;隔开，\n比如：ip1:port1;ip2:port2");
        this.url.addModifyListener(lsMod);
        this.urlTxtFormData = new FormData();
        this.urlTxtFormData.left = new FormAttachment(middle, 0);
        this.urlTxtFormData.right = new FormAttachment(100, 0);
        this.urlTxtFormData.top = new FormAttachment(this.outputTypeLabel, 8);//距上一个控件的位置
        this.url.setLayoutData(this.urlTxtFormData);
        
        //For blockSize
        this.blockSizeLabel = new Label(this.shell, 131072);
        this.blockSizeLabel.setText("块大小(MB)");
        this.props.setLook(this.blockSizeLabel);
        this.blockSizeFormData = new FormData();
        this.blockSizeFormData.left = new FormAttachment(0, 0);
        this.blockSizeFormData.right = new FormAttachment(middle, -margin);
        this.blockSizeFormData.top = new FormAttachment(this.urlLabel, margin);//距上一个控件的位置
        this.blockSizeLabel.setLayoutData(this.blockSizeFormData);
        
        this.blockSize = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.blockSize);
        this.blockSize.addModifyListener(lsMod);
        this.blockSizeTxtFormData = new FormData();
        this.blockSizeTxtFormData.left = new FormAttachment(middle, 0);
        this.blockSizeTxtFormData.right = new FormAttachment(100, 0);
        this.blockSizeTxtFormData.top = new FormAttachment(this.urlLabel, margin);//距上一个控件的位置
        this.blockSize.setLayoutData(this.blockSizeTxtFormData);
        
        //For pageSize
        this.pageSizeLabel = new Label(this.shell, 131072);
        this.pageSizeLabel.setText("页大小(KB)");
        this.props.setLook(this.pageSizeLabel);
        this.pageSizeFormData = new FormData();
        this.pageSizeFormData.left = new FormAttachment(0, 0);
        this.pageSizeFormData.right = new FormAttachment(middle, -margin);
        this.pageSizeFormData.top = new FormAttachment(this.blockSize, margin);//距上一个控件的位置
        this.pageSizeLabel.setLayoutData(this.pageSizeFormData);
        
        this.pageSize = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        this.props.setLook(this.pageSize);
        this.pageSize.addModifyListener(lsMod);
        this.pageSizeTxtFormData = new FormData();
        this.pageSizeTxtFormData.left = new FormAttachment(middle, 0);
        this.pageSizeTxtFormData.right = new FormAttachment(100, 0);
        this.pageSizeTxtFormData.top = new FormAttachment(this.blockSize, margin);//距上一个控件的位置
        this.pageSize.setLayoutData(this.pageSizeTxtFormData);
        
        // ////////////////////// //
        // 创建Hive表并导入数据配置组 //
        // ////////////////////// //
        
        wHiveGroup = new Group(shell, SWT.SHADOW_NONE);
        props.setLook( wHiveGroup );
        wHiveGroup.setText("Hive表配置");

        FormLayout HiveGroupLayout = new FormLayout();
        HiveGroupLayout.marginWidth = 10;
        HiveGroupLayout.marginHeight = 10;
        wHiveGroup.setLayout(HiveGroupLayout);
        
        //创建表并导入数据？
        this.wlCreateAndLoad = new Label(wHiveGroup, SWT.RIGHT );
        this.wlCreateAndLoad.setText("创建Hive表并导入数据？");
        this.props.setLook(this.wlCreateAndLoad);
        this.fdlCreateAndLoad = new FormData();
        this.fdlCreateAndLoad.left = new FormAttachment(0, 0);
        this.fdlCreateAndLoad.right = new FormAttachment(middle, -margin);
        this.fdlCreateAndLoad.top = new FormAttachment(this.pageSize, margin);
        this.wlCreateAndLoad.setLayoutData(this.fdlCreateAndLoad);

        this.wCreateAndLoad = new Button(wHiveGroup, SWT.CHECK);
        this.wCreateAndLoad.setToolTipText("勾选后，如果表已经存在则直接导入数据，否则将创建表并导入数据。");
        this.props.setLook(this.wCreateAndLoad);
        this.fdCreateAndLoad = new FormData();
        this.fdCreateAndLoad.left = new FormAttachment(middle, 0);
        this.fdCreateAndLoad.top = new FormAttachment(this.pageSize, margin*2);
        this.fdCreateAndLoad.right = new FormAttachment(100, 0);
        this.wCreateAndLoad.setLayoutData(this.fdCreateAndLoad);
        this.wCreateAndLoad.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
              input.setChanged();
              enableCreateAndLoad();
            }
        });
        
        //Hive版本
        this.wlHiveVersion = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveVersion.setText("HiveServer版本");
        this.props.setLook(this.wlHiveVersion);
        this.fdlHiveVersion = new FormData();
        this.fdlHiveVersion.left = new FormAttachment(0, 0);
        this.fdlHiveVersion.right = new FormAttachment(middle, -margin);
        this.fdlHiveVersion.top = new FormAttachment(this.wCreateAndLoad, margin);
        this.wlHiveVersion.setLayoutData(this.fdlHiveVersion);
        
        this.wHiveVersion = new CCombo(this.wHiveGroup, SWT.BORDER | SWT.READ_ONLY ); 
        this.wHiveVersion.setToolTipText("hive2需要启动HiveServer2服务，hive需要启动HiveServer服务！");
        this.wHiveVersion.add("impala");
        this.wHiveVersion.add("hive2");
        this.wHiveVersion.add("hive");
        this.wHiveVersion.select(0); 
        this.props.setLook(this.wHiveVersion);
        this.wHiveVersion.addModifyListener(lsMod);
        this.fdHiveVersion = new FormData();
        this.fdHiveVersion.left = new FormAttachment(middle, 0);
        this.fdHiveVersion.right = new FormAttachment(100, 0);
        this.fdHiveVersion.top = new FormAttachment(this.wCreateAndLoad, margin);
        this.wHiveVersion.setLayoutData(this.fdHiveVersion);
        this.wHiveVersion.addModifyListener(new ModifyListener() {
        	public void modifyText( ModifyEvent e ) {
                input.setChanged();
                if (wHiveVersion.getText().equals("impala")) {
                    wHiveUrl.setText("jdbc:hive2://172.16.50.21:21050/test;auth=noSasl");
                } else if (wHiveVersion.getText().equals("hive2")) {
                    wHiveUrl.setText("jdbc:hive2://172.16.50.22:10000/test");
                } else {
                    wHiveUrl.setText("jdbc:hive://172.16.50.22:10000/test");
                }
            }
		});
        
        /*//Hive主机地址
        this.wlHiveHost = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveHost.setText("主机地址");
        this.props.setLook(this.wlHiveHost);
        this.fdlHiveHost = new FormData();
        this.fdlHiveHost.left = new FormAttachment(0, 0);
        this.fdlHiveHost.right = new FormAttachment(middle, -margin);
        this.fdlHiveHost.top = new FormAttachment(this.wHiveVersion, margin);
        this.wlHiveHost.setLayoutData(this.fdlHiveHost);

        this.wHiveHost = new Text(this.wHiveGroup, 18436);
        this.props.setLook(this.wHiveHost);
        this.wHiveHost.addModifyListener(lsMod);
        this.fdHiveHost = new FormData();
        this.fdHiveHost.left = new FormAttachment(middle, 0);
        this.fdHiveHost.right = new FormAttachment(100, 0);
        this.fdHiveHost.top = new FormAttachment(this.wHiveVersion, margin);
        this.wHiveHost.setLayoutData(this.fdHiveHost);

        //Hive端口号
        this.wlHivePort = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHivePort.setText("端口号");
        this.props.setLook(this.wlHivePort);
        this.fdlHivePort = new FormData();
        this.fdlHivePort.left = new FormAttachment(0, 0);
        this.fdlHivePort.right = new FormAttachment(middle, -margin);
        this.fdlHivePort.top = new FormAttachment(this.wHiveHost, margin);
        this.wlHivePort.setLayoutData(this.fdlHivePort);

        this.wHivePort = new Text(this.wHiveGroup, 18436);
        this.props.setLook(this.wHivePort);
        this.wHivePort.addModifyListener(lsMod);
        this.fdHivePort = new FormData();
        this.fdHivePort.left = new FormAttachment(middle, 0);
        this.fdHivePort.right = new FormAttachment(100, 0);
        this.fdHivePort.top = new FormAttachment(this.wHiveHost, margin);
        this.wHivePort.setLayoutData(this.fdHivePort);

        //Hive数据库名称
        this.wlHiveDB = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveDB.setText("数据库名称");
        this.props.setLook(this.wlHiveDB);
        this.fdlHiveDB = new FormData();
        this.fdlHiveDB.left = new FormAttachment(0, 0);
        this.fdlHiveDB.right = new FormAttachment(middle, -margin);
        this.fdlHiveDB.top = new FormAttachment(this.wHivePort, margin);
        this.wlHiveDB.setLayoutData(this.fdlHiveDB);

        this.wHiveDB = new Text(this.wHiveGroup, SWT.BORDER);
        this.props.setLook(this.wHiveDB);
        this.wHiveDB.addModifyListener(lsMod);
        this.fdHiveDB = new FormData();
        this.fdHiveDB.left = new FormAttachment(middle, 0);
        this.fdHiveDB.right = new FormAttachment(100, 0);
        this.fdHiveDB.top = new FormAttachment(this.wHivePort, margin);
        this.wHiveDB.setLayoutData(this.fdHiveDB);*/

        // jdbc url
        this.wlHiveUrl = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveUrl.setText("hiveUrl");
        this.props.setLook(this.wlHiveUrl);
        FormData fdlUrl = new FormData();
        fdlUrl.left = new FormAttachment(0, 0);
        fdlUrl.right = new FormAttachment(middle, -margin);
        fdlUrl.top = new FormAttachment(this.wHiveVersion, margin);
        this.wlHiveUrl.setLayoutData(fdlUrl);

        this.wHiveUrl = new Text(this.wHiveGroup, SWT.BORDER);
        this.props.setLook(this.wHiveUrl);
        this.wHiveUrl.addModifyListener(lsMod);
        FormData fdUrl = new FormData();
        fdUrl.left = new FormAttachment(middle, 0);
        fdUrl.right = new FormAttachment(100, 0);
        fdUrl.top = new FormAttachment(this.wHiveVersion, margin);
        this.wHiveUrl.setLayoutData(fdUrl);
        
        //Hive用户名
        this.wlHiveUser = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveUser.setText("用户名");
        this.props.setLook(this.wlHiveUser);
        this.fdlHiveUser = new FormData();
        this.fdlHiveUser.left = new FormAttachment(0, 0);
        this.fdlHiveUser.right = new FormAttachment(middle, -margin);
        this.fdlHiveUser.top = new FormAttachment(this.wHiveUrl, margin);
        this.wlHiveUser.setLayoutData(this.fdlHiveUser);
        
        this.wHiveUser = new Text(this.wHiveGroup, SWT.BORDER);
        this.props.setLook(this.wHiveUser);
        this.wHiveUser.addModifyListener(lsMod);
        this.fdHiveUser = new FormData();
        this.fdHiveUser.left = new FormAttachment(middle, 0);
        this.fdHiveUser.right = new FormAttachment(100, 0);
        this.fdHiveUser.top = new FormAttachment(this.wHiveUrl, margin);
        this.wHiveUser.setLayoutData(this.fdHiveUser);
        
        //Hive密码
        this.wlHivePassword = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHivePassword.setText("密码");
        this.props.setLook(this.wlHivePassword);
        this.fdlHivePassword = new FormData();
        this.fdlHivePassword.left = new FormAttachment(0, 0);
        this.fdlHivePassword.right = new FormAttachment(middle, -margin);
        this.fdlHivePassword.top = new FormAttachment(this.wHiveUser, margin);
        this.wlHivePassword.setLayoutData(this.fdlHivePassword);
        
        this.wHivePassword = new Text(this.wHiveGroup, SWT.BORDER | SWT.PASSWORD);
        this.props.setLook(this.wHivePassword);
        this.wHivePassword.addModifyListener(lsMod);
        this.fdHivePassword = new FormData();
        this.fdHivePassword.left = new FormAttachment(middle, 0);
        this.fdHivePassword.right = new FormAttachment(100, 0);
        this.fdHivePassword.top = new FormAttachment(this.wHiveUser, margin);
        this.wHivePassword.setLayoutData(this.fdHivePassword);        
        
        //Hive表
        this.wlHiveTable = new Label(this.wHiveGroup, SWT.RIGHT);
        this.wlHiveTable.setText("表名");
        this.props.setLook(this.wlHiveTable);
        this.fdlHiveTable = new FormData();
        this.fdlHiveTable.left = new FormAttachment(0, 0);
        this.fdlHiveTable.right = new FormAttachment(middle, -margin);
        this.fdlHiveTable.top = new FormAttachment(this.wHivePassword, margin);
        this.wlHiveTable.setLayoutData(this.fdlHiveTable);
        
        this.wHiveTable = new Text(this.wHiveGroup, SWT.BORDER);
        this.props.setLook(this.wHiveTable);
        this.wHiveTable.addModifyListener(lsMod);
        this.fdHiveTable = new FormData();
        this.fdHiveTable.left = new FormAttachment(middle, 0);
        this.fdHiveTable.right = new FormAttachment(100, 0);
        this.fdHiveTable.top = new FormAttachment(this.wHivePassword, margin);
        this.wHiveTable.setLayoutData(this.fdHiveTable);
        
        //覆盖原表？
        this.wlOverwriteTable = new Label(wHiveGroup, SWT.RIGHT );
        this.wlOverwriteTable.setText("覆盖原表？");
        this.props.setLook(this.wlOverwriteTable);
        this.fdlOverwriteTable = new FormData();
        this.fdlOverwriteTable.left = new FormAttachment(0, 0);
        this.fdlOverwriteTable.right = new FormAttachment(middle, -margin);
        this.fdlOverwriteTable.top = new FormAttachment(this.wHiveTable, margin);
        this.wlOverwriteTable.setLayoutData(this.fdlOverwriteTable);

        this.wOverwriteTable = new Button(wHiveGroup, SWT.CHECK);
        this.wOverwriteTable.setToolTipText("当Hive表已经存在时，如果勾选则以覆盖方式导入数据，\n否则以追加的方式导入数据。");
        this.props.setLook(this.wOverwriteTable);
        this.fdOverwriteTable = new FormData();
        this.fdOverwriteTable.left = new FormAttachment(middle, 0);
        this.fdOverwriteTable.top = new FormAttachment(this.wHiveTable, margin);
        this.fdOverwriteTable.right = new FormAttachment(100, 0);
        this.wOverwriteTable.setLayoutData(this.fdOverwriteTable);
        this.wOverwriteTable.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
              input.setChanged();
            }
        });
        
        //执行SQL脚本？
        this.wlExecuteSQL = new Label(wHiveGroup, SWT.RIGHT );
        this.wlExecuteSQL.setText("执行SQL脚本？");
        this.props.setLook(this.wlExecuteSQL);
        this.fdlExecuteSQL = new FormData();
        this.fdlExecuteSQL.left = new FormAttachment(0, 0);
        this.fdlExecuteSQL.right = new FormAttachment(middle, -margin);
        this.fdlExecuteSQL.top = new FormAttachment(this.wOverwriteTable, margin);
        this.wlExecuteSQL.setLayoutData(this.fdlExecuteSQL);

        this.wExecuteSQL = new Button(wHiveGroup, SWT.CHECK);
        this.wExecuteSQL.setToolTipText("数据导入Hive后，执行指定SQL脚本");
        this.props.setLook(this.wExecuteSQL);
        this.fdExecuteSQL = new FormData();
        this.fdExecuteSQL.left = new FormAttachment(middle, 0);
        this.fdExecuteSQL.top = new FormAttachment(this.wOverwriteTable, margin);
        this.fdExecuteSQL.right = new FormAttachment(100, 0);
        this.wExecuteSQL.setLayoutData(this.fdExecuteSQL);
        this.wExecuteSQL.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
              input.setChanged();
              enableSqlContent();
            }
        });
        
        //For sqlContent
        this.sqlContentLabel = new Label(this.wHiveGroup, 131072);
        this.sqlContentLabel.setText("SQL脚本");
        this.props.setLook(this.sqlContentLabel);
        this.sqlContentFormData = new FormData();
        this.sqlContentFormData.left = new FormAttachment(0, 0);
        this.sqlContentFormData.right = new FormAttachment(middle, -margin);
        this.sqlContentFormData.top = new FormAttachment(this.wExecuteSQL, margin);//距上一个控件的位置
        this.sqlContentLabel.setLayoutData(this.sqlContentFormData);
        
        this.sqlContent = new StyledTextComp( transMeta, wHiveGroup, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "");
        this.props.setLook(this.sqlContent);
        this.sqlContent.addModifyListener(lsMod);
        this.sqlContent.addLineStyleListener( new SQLValuesHighlight());
        this.sqlContentTxtFormData = new FormData();
        this.sqlContentTxtFormData.left = new FormAttachment(middle, 0);
        this.sqlContentTxtFormData.right = new FormAttachment(100, 0);
        this.sqlContentTxtFormData.top = new FormAttachment(this.wExecuteSQL, margin);//距上一个控件的位置
        this.sqlContentTxtFormData.height = 120;
        this.sqlContent.setLayoutData(this.sqlContentTxtFormData);

        fdHiveGroup = new FormData();
        fdHiveGroup.left = new FormAttachment(0, margin);
        fdHiveGroup.top = new FormAttachment(this.pageSize, margin*2);
        fdHiveGroup.right = new FormAttachment(100, -margin);
        wHiveGroup.setLayoutData(fdHiveGroup);
        

        // For Button
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        
        this.wTestConnect = new Button(this.shell, 8);
        this.wTestConnect.setText("测试Hive连接");

        BaseStepDialog.positionBottomButtons(this.shell,
        		new Button[]{wOK, wCancel, wTestConnect}, margin, wHiveGroup);

        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                OrcParquetOutDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                OrcParquetOutDialog.this.ok();
            }
        };
        

        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);
        this.wTestConnect.addListener(13, new Listener() {
			public void handleEvent(Event arg0) {
				testConnect();
			}
		});

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                OrcParquetOutDialog.this.shell.dispose();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                OrcParquetOutDialog.this.shell.dispose();
            }
        });
        setSize();

        getData();
        this.input.setChanged(this.changed);

        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return this.stepname;
    }

    protected void testConnect() {
    	MessageBox mb = new MessageBox(shell);
    	try {
			String driverName = "org.apache.hive.jdbc.HiveDriver";
			if(wHiveVersion.getText().equals("hive")){
				 driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
			}
			Class.forName(driverName);
			/*String url = "jdbc:" + wHiveVersion.getText() + "://" + wHiveHost.getText() + ":"
					+ wHivePort.getText() + "/" + wHiveDB.getText();*/
			String url = wHiveUrl.getText();
			if (wHiveVersion.getText().equals("impala")) {
			    if (!url.contains("auth=")) {
                    url += ";auth=noSasl";
                }
            }
			Connection con = DriverManager.getConnection(url, wHiveUser.getText(), wHivePassword.getText());
			Statement stmt = con.createStatement();
			stmt.executeQuery("show tables");
	    	con.close();
	    	
	    	mb.setMessage("连接Hive成功！" + "\nurl："+ url);
		} catch (Exception e) {
			mb.setMessage("连接Hive失败！\n" + e.getMessage());
		}
    	mb.open();
	}

	protected void enableCreateAndLoad() {
    	wlHiveVersion.setEnabled(wCreateAndLoad.getSelection());
        wHiveVersion.setEnabled(wCreateAndLoad.getSelection());
/*        wlHiveHost.setEnabled(wCreateAndLoad.getSelection());
        wHiveHost.setEnabled(wCreateAndLoad.getSelection());
        wlHivePort.setEnabled(wCreateAndLoad.getSelection());
        wHivePort.setEnabled(wCreateAndLoad.getSelection());
        wlHiveDB.setEnabled(wCreateAndLoad.getSelection());
        wHiveDB.setEnabled(wCreateAndLoad.getSelection());*/
        wlHiveUrl.setEnabled(wCreateAndLoad.getSelection());
        wHiveUrl.setEnabled(wCreateAndLoad.getSelection());
        wlHiveUser.setEnabled(wCreateAndLoad.getSelection());
        wHiveUser.setEnabled(wCreateAndLoad.getSelection());
        wlHivePassword.setEnabled(wCreateAndLoad.getSelection());
        wHivePassword.setEnabled(wCreateAndLoad.getSelection());
        wlHiveTable.setEnabled(wCreateAndLoad.getSelection());
        wHiveTable.setEnabled(wCreateAndLoad.getSelection());
        wlOverwriteTable.setEnabled(wCreateAndLoad.getSelection());
        wOverwriteTable.setEnabled(wCreateAndLoad.getSelection());
        wTestConnect.setEnabled(wCreateAndLoad.getSelection());
        wlExecuteSQL.setEnabled(wCreateAndLoad.getSelection());
        wExecuteSQL.setEnabled(wCreateAndLoad.getSelection());
	}
    
    protected void enablePagesize() {
		boolean enablePagesize = "parquet".equals(outputType.getText());
		pageSizeLabel.setEnabled(enablePagesize);
        pageSize.setEnabled(enablePagesize);
	}
    
    protected void enableSqlContent() {
    	sqlContentLabel.setEnabled(wExecuteSQL.getSelection());
    	sqlContent.setEnabled(wExecuteSQL.getSelection());
    }

	public void getData() {
        this.wStepname.selectAll();
        this.fileName.setText(this.input.getFileName());
        this.wCleanOutput.setSelection(this.input.isCleanOutput());
        this.outputType.setText(this.input.getOutputType());
        this.url.setText(this.input.getUrl());
        this.blockSize.setText(this.input.getBlockSize());
        this.pageSize.setText(this.input.getPageSize());
        this.wCreateAndLoad.setSelection(input.isCreateAndLoad());
        this.wHiveVersion.setText(input.getHiveVersion());
/*        this.wHiveHost.setText(input.getHiveHost());
        this.wHivePort.setText(input.getHivePort());
        this.wHiveDB.setText(input.getHiveDB());*/
        this.wHiveUrl.setText(input.getHiveUrl());
        this.wHiveUser.setText(input.getHiveUser());
        this.wHivePassword.setText(input.getHivePassword());
        this.wHiveTable.setText(input.getHiveTable());
        this.wOverwriteTable.setSelection(input.isOverwriteTable());
        this.wExecuteSQL.setSelection(input.getExcuteSql());
        this.sqlContent.setText(input.getSqlContent());
        
        enableCreateAndLoad();
        enablePagesize();
        enableSqlContent();
    }

    private void cancel() {
        this.stepname = null;
        this.input.setChanged(this.changed);
        dispose();
    }

    private void ok() {
    	if (Const.isEmpty(wStepname.getText())) {
    		return;
    	}
        this.stepname = this.wStepname.getText();
        this.input.setFileName(this.fileName.getText());
        this.input.setCleanOutput(this.wCleanOutput.getSelection());
        this.input.setOutputType(this.outputType.getText());
        this.input.setUrl(this.url.getText());
        this.input.setBlockSize(this.blockSize.getText());
        this.input.setPageSize(this.pageSize.getText());
        this.input.setCreateAndLoad(wCreateAndLoad.getSelection());
        this.input.setHiveVersion(wHiveVersion.getText());
/*        this.input.setHiveHost(wHiveHost.getText());
        this.input.setHivePort(wHivePort.getText());
        this.input.setHiveDB(wHiveDB.getText());*/
        this.input.setHiveUrl(wHiveUrl.getText());
        this.input.setHiveUser(wHiveUser.getText());
        this.input.setHivePassword(wHivePassword.getText());
        this.input.setHiveTable(wHiveTable.getText());
        this.input.setOverwriteTable(wOverwriteTable.getSelection());
        this.input.setExcuteSql(wExecuteSQL.getSelection());
        this.input.setSqlContent(sqlContent.getText());

        dispose();
    }

    /*@Override
    protected Button createHelpButton( Shell shell, StepMeta stepMeta, PluginInterface plugin ) {
      return HelpUtils.createHelpButton( helpComp, HelpUtils.getHelpDialogTitle( plugin ), plugin );
    }*/
}