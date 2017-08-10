package com.chinacloud.esoutput;


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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class EDBOutDialog extends BaseStepDialog
        implements StepDialogInterface {
    private static Class<?> PKG = EDBOutMeta.class;
    private EDBOutMeta input;

    private Label wlOutputType;
    private CCombo wOutputType;
    private FormData fdlOutputType, fdOutputType;
    
    private Label wlUrl;
    private Text wUrl;
    private FormData fdlUrl, fdUrl;
    
    private Label wlClusterName;
    private Text wClusterName;
    private FormData fdlClusterName, fdClusterName;

    private Label wlDatabase;
    private Text wDatabase;
    private FormData fdlDatabase, fdDatabase;
    
    private Label wlTable;
    private Text wTable;
    private FormData fdlTable, fdTable;
    
    private Label wlFieldName;
    private Text wFieldName;
    private FormData fdlFieldName, fdFieldName;
    
    private Label wlStatFieldName;
    private Text wStatFieldName;
    private FormData fdlStatFieldName, fdStatFieldName;
    
    private Label wlStatCnd;
    private Text wStatCnd;
    private FormData fdlStatCnd, fdStatCnd;
    
    private Label wlBatchSize;
    private Text wBatchSize;
    private FormData fdlBatchSize, fdBatchSize;
    
	private Group wServersGroup;
	private TableView wServers;
	private ModifyListener lsMod;
	private Button wTestConnection;

    public EDBOutDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.input = ((EDBOutMeta) in);
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);
        this.shell.setSize(200, 300);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.input);

        this.lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                EDBOutDialog.this.input.setChanged();
            }
        };
        this.changed = this.input.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        
        this.shell.setLayout(formLayout);

        this.shell.setText("ES输出配置");

        int middle = this.props.getMiddlePct();
        middle -= 10;
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
        
        
        //For outputTypeLabel
        this.wlOutputType = new Label(this.shell, 131072);
        this.wlOutputType.setText("输出类型");
        this.wlOutputType.setToolTipText("add-only选项直接添加每一行数据，会覆盖旧数据和其他列，\n"
        		+ "outer-update选项会新增/修改当前数据流中的字段值到表中，不会覆盖其他列，\n"
        		+ "inner-update选项会将数据流中所有行作为内嵌对象更新到表的指定字段中");
        this.props.setLook(this.wlOutputType);
        this.fdlOutputType = new FormData();
        this.fdlOutputType.left = new FormAttachment(0, 0);
        this.fdlOutputType.right = new FormAttachment(middle, -margin);
        this.fdlOutputType.top = new FormAttachment(this.wStepname, margin);
        this.wlOutputType.setLayoutData(this.fdlOutputType);

        this.wOutputType = new CCombo(this.shell, SWT.READ_ONLY); //定义一个只读的下拉框
        this.wOutputType.add("add-only");
        this.wOutputType.add("outer-update");
        this.wOutputType.add("inner-update");
        this.wOutputType.setToolTipText("add-only选项直接添加每一行数据，会覆盖旧数据和其他列，\n"
        		+ "outer-update选项会新增/修改当前数据流中的字段值到表中，不会覆盖其他列，\n"
        		+ "inner-update选项会将数据流中所有行作为内嵌对象更新到表的指定字段中");
        this.wOutputType.select(0); 
        this.props.setLook(this.wOutputType);
        this.wOutputType.addModifyListener(lsMod);
        this.fdOutputType = new FormData();
        this.fdOutputType.left = new FormAttachment(middle, 0);
        this.fdOutputType.right = new FormAttachment(100, 0);
        this.fdOutputType.top = new FormAttachment(this.wStepname, margin);
        this.wOutputType.setLayoutData(this.fdOutputType);
        this.wOutputType.addSelectionListener( new SelectionAdapter() {
            public void widgetSelected( SelectionEvent e ) {
              input.setChanged();
              enableFieldName();
            }
        });

        //For Url
        this.wlUrl = new Label(this.shell, 131072);
        this.wlUrl.setText("ElasticSearch地址端口");
        this.wlUrl.setToolTipText("ElasticSearch的地址和端口");
        this.props.setLook(this.wlUrl);
        this.fdlUrl = new FormData();
        this.fdlUrl.left = new FormAttachment(0, 0);
        this.fdlUrl.right = new FormAttachment(middle, -margin);
        this.fdlUrl.top = new FormAttachment(this.wlOutputType, 8);//距上一个控件的位置
        this.wlUrl.setLayoutData(this.fdlUrl);

        this.wUrl = new Text(this.shell, 18436);
        this.props.setLook(this.wUrl);
        this.wUrl.setToolTipText("节点的地址和端口，多个之间用英文分号;隔开，\n比如：ip1:port1;ip2:port2");
        this.wUrl.addModifyListener(lsMod);
        this.fdUrl = new FormData();
        this.fdUrl.left = new FormAttachment(middle, 0);
        this.fdUrl.right = new FormAttachment(100, 0);
        this.fdUrl.top = new FormAttachment(this.wlOutputType, 8);//距上一个控件的位置
        this.wUrl.setLayoutData(this.fdUrl);
        
        //ClusterName
        this.wlClusterName = new Label(this.shell, SWT.RIGHT);
        this.wlClusterName.setText("集群名称");
        this.props.setLook(this.wlClusterName);
        this.fdlClusterName = new FormData();
        this.fdlClusterName.left = new FormAttachment(0, 0);
        this.fdlClusterName.right = new FormAttachment(middle, -margin);
        this.fdlClusterName.top = new FormAttachment(this.wUrl, margin);
        this.wlClusterName.setLayoutData(this.fdlClusterName);
        
        this.wClusterName = new Text(this.shell, 18436);
        this.props.setLook(this.wClusterName);
        this.wClusterName.addModifyListener(lsMod);
        this.fdClusterName = new FormData();
        this.fdClusterName.left = new FormAttachment(middle, 0);
        this.fdClusterName.right = new FormAttachment(100, 0);
        this.fdClusterName.top = new FormAttachment(this.wUrl, margin);
        this.wClusterName.setLayoutData(this.fdClusterName);
        
        //For Database
        this.wlDatabase = new Label(this.shell, 131072);
        this.wlDatabase.setText("索引名");
        this.props.setLook(this.wlDatabase);
        this.fdlDatabase = new FormData();
        this.fdlDatabase.left = new FormAttachment(0, 0);
        this.fdlDatabase.right = new FormAttachment(middle, -margin);
        this.fdlDatabase.top = new FormAttachment(this.wClusterName, margin);//距上一个控件的位置
        this.wlDatabase.setLayoutData(this.fdlDatabase);
        
        this.wDatabase = new Text(this.shell, 18436);
        this.props.setLook(this.wDatabase);
        this.wDatabase.addModifyListener(lsMod);
        this.fdDatabase = new FormData();
        this.fdDatabase.left = new FormAttachment(middle, 0);
        this.fdDatabase.right = new FormAttachment(100, 0);
        this.fdDatabase.top = new FormAttachment(this.wClusterName, margin);//距上一个控件的位置
        this.wDatabase.setLayoutData(this.fdDatabase);
        
        //For Table
        this.wlTable = new Label(this.shell, 131072);
        this.wlTable.setText("类型（表名）");
        this.props.setLook(this.wlTable);
        this.fdlTable = new FormData();
        this.fdlTable.left = new FormAttachment(0, 0);
        this.fdlTable.right = new FormAttachment(middle, -margin);
        this.fdlTable.top = new FormAttachment(this.wlDatabase, margin);//距上一个控件的位置
        this.wlTable.setLayoutData(this.fdlTable);
        
        this.wTable = new Text(this.shell, 18436);
        this.props.setLook(this.wTable);
        this.wTable.addModifyListener(lsMod);
        this.fdTable = new FormData();
        this.fdTable.left = new FormAttachment(middle, 0);
        this.fdTable.right = new FormAttachment(100, 0);
        this.fdTable.top = new FormAttachment(this.wlDatabase, margin);//距上一个控件的位置
        this.wTable.setLayoutData(this.fdTable);
        
        //For FieldName
        this.wlFieldName = new Label(this.shell, 131072);
        this.wlFieldName.setEnabled(false);
        this.wlFieldName.setText("ES中更新字段名");
        this.props.setLook(this.wlFieldName);
        this.fdlFieldName = new FormData();
        this.fdlFieldName.left = new FormAttachment(0, 0);
        this.fdlFieldName.right = new FormAttachment(middle, -margin);
        this.fdlFieldName.top = new FormAttachment(this.wlTable, margin);//距上一个控件的位置
        this.wlFieldName.setLayoutData(this.fdlFieldName);
        
        this.wFieldName = new Text(this.shell, 18436);
        this.wFieldName.setEnabled(false);
        this.props.setLook(this.wFieldName);
        this.wFieldName.addModifyListener(lsMod);
        this.fdFieldName = new FormData();
        this.fdFieldName.left = new FormAttachment(middle, 0);
        this.fdFieldName.right = new FormAttachment(100, 0);
        this.fdFieldName.top = new FormAttachment(this.wlTable, margin);//距上一个控件的位置
        this.wFieldName.setLayoutData(this.fdFieldName);
        
        //For StatFieldName
        this.wlStatFieldName = new Label(this.shell, 131072);
        this.wlStatFieldName.setEnabled(false);
        this.wlStatFieldName.setText("ES中统计字段名");
        this.props.setLook(this.wlStatFieldName);
        this.fdlStatFieldName = new FormData();
        this.fdlStatFieldName.left = new FormAttachment(0, 0);
        this.fdlStatFieldName.right = new FormAttachment(middle, -margin);
        this.fdlStatFieldName.top = new FormAttachment(this.wFieldName, margin);//距上一个控件的位置
        this.wlStatFieldName.setLayoutData(this.fdlStatFieldName);
        
        this.wStatFieldName = new Text(this.shell, 18436);
        this.wStatFieldName.setEnabled(false);
        this.props.setLook(this.wStatFieldName);
        this.wStatFieldName.addModifyListener(lsMod);
        this.fdStatFieldName = new FormData();
        this.fdStatFieldName.left = new FormAttachment(middle, 0);
        this.fdStatFieldName.right = new FormAttachment(100, 0);
        this.fdStatFieldName.top = new FormAttachment(this.wFieldName, margin);//距上一个控件的位置
        this.wStatFieldName.setLayoutData(this.fdStatFieldName);
        
        //For StatCnd
        this.wlStatCnd = new Label(this.shell, 131072);
        this.wlStatCnd.setEnabled(false);
        this.wlStatCnd.setText("统计条件");
        this.wlStatCnd.setToolTipText("统计字段条件格式为：fieldName opt value，opt为比较符，支持：=,!=,<,>");
        this.props.setLook(this.wlStatCnd);
        this.fdlStatCnd = new FormData();
        this.fdlStatCnd.left = new FormAttachment(0, 0);
        this.fdlStatCnd.right = new FormAttachment(middle, -margin);
        this.fdlStatCnd.top = new FormAttachment(this.wStatFieldName, margin);//距上一个控件的位置
        this.wlStatCnd.setLayoutData(this.fdlStatCnd);
        
        this.wStatCnd = new Text(this.shell, 18436);
        this.wStatCnd.setEnabled(false);
        this.wStatCnd.setToolTipText("统计字段条件格式为：fieldName opt value，opt为比较符，支持：=,!=,<,>\n"
        		+ "如果为<或>，则比较字段值必须是数字！");
        this.props.setLook(this.wStatCnd);
        this.wStatCnd.addModifyListener(lsMod);
        this.fdStatCnd = new FormData();
        this.fdStatCnd.left = new FormAttachment(middle, 0);
        this.fdStatCnd.right = new FormAttachment(100, 0);
        this.fdStatCnd.top = new FormAttachment(this.wStatFieldName, margin);//距上一个控件的位置
        this.wStatCnd.setLayoutData(this.fdStatCnd);
        
        //For BatchSize
        this.wlBatchSize = new Label(this.shell, 131072);
        this.wlBatchSize.setText("提交批量(条)");
        this.props.setLook(this.wlBatchSize);
        this.fdlBatchSize = new FormData();
        this.fdlBatchSize.left = new FormAttachment(0, 0);
        this.fdlBatchSize.right = new FormAttachment(middle, -margin);
        this.fdlBatchSize.top = new FormAttachment(this.wStatCnd, margin);//距上一个控件的位置
        this.wlBatchSize.setLayoutData(this.fdlBatchSize);
        
        this.wBatchSize = new Text(this.shell, 18436);
        this.props.setLook(this.wBatchSize);
        this.wBatchSize.addModifyListener(lsMod);
        this.fdBatchSize = new FormData();
        this.fdBatchSize.left = new FormAttachment(middle, 0);
        this.fdBatchSize.right = new FormAttachment(100, 0);
        this.fdBatchSize.top = new FormAttachment(this.wStatCnd, margin);//距上一个控件的位置
        this.wBatchSize.setLayoutData(this.fdBatchSize);

        // For Button
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        
        this.wTestConnection = new Button(this.shell, 8);
        this.wTestConnection.setText("测试ES连接");

        BaseStepDialog.positionBottomButtons(this.shell,
        		new Button[]{wOK, wCancel, wTestConnection}, margin, this.wBatchSize);

        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                EDBOutDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                EDBOutDialog.this.ok();
            }
        };

        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);
        this.wTestConnection.addListener(13, new Listener() {
			public void handleEvent(Event arg0) {
				testConnect();
			}
		});

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                EDBOutDialog.this.shell.dispose();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                EDBOutDialog.this.shell.dispose();
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

	protected void enableFieldName() {
		boolean flag = "inner-update".equals(wOutputType.getText());
		this.wFieldName.setEnabled(flag);
		this.wlFieldName.setEnabled(flag);
		this.wStatFieldName.setEnabled(flag);
		this.wlStatFieldName.setEnabled(flag);
		this.wStatCnd.setEnabled(flag);
		this.wlStatCnd.setEnabled(flag);
	}

	/**
     * 测试es连接
     */
    protected void testConnect() {
    	String clusterName = wClusterName.getText();
    	String index = wDatabase.getText();
    	String[] urls = wUrl.getText().split(";");
		String result = "";
		MessageBox mb = new MessageBox(shell);
		try {
			for (String url : urls) {
				String[] ip_port = url.split(":");
				String ip = ip_port[0];
				int port = Integer.parseInt(ip_port[1]);
				String msg = new SQL4ESUtil(clusterName, ip, port).testServer(index);
				if(msg.equals("OK")){
					result += "\n"+url+" [SUCCESS]";
				}else{
					result += "\n"+url+" [FAIL]: "+msg;
				}
			}
			mb.setMessage("连接测试结果如下："+ result);
		} catch (Exception e) {
			mb.setMessage(e.getMessage());
		}
		mb.open();
	}
    
    /**
     * 自动创建index
     * @return 
     */
    protected boolean createIndex() {
    	String clusterName = wClusterName.getText();
    	String index = wDatabase.getText();
    	String[] urls = wUrl.getText().split(";");
    	String[] ips = new String[urls.length];
    	int[] ports = new int[urls.length];
    	for (int i=0;i<urls.length;i++) {
			String[] ip_port = urls[i].split(":");
			ips[i] = ip_port[0];
			ports[i] = Integer.parseInt(ip_port[1]);
		}
    	return new SQL4ESUtil(clusterName, ips, ports).createIndex(index);
	}

	public void getData() {
        this.wStepname.selectAll();
        this.wOutputType.setText(input.getOutputType());
        this.wUrl.setText(input.getUrl());
        this.wClusterName.setText(input.getClusterName());
		if (input.getDatabase() != null) {
			this.wDatabase.setText(input.getDatabase());
		}
		if (input.getOutputTable() != null) {
			this.wTable.setText(input.getOutputTable());
		}
		if (input.getFieldName() != null) {
			this.wFieldName.setText(input.getFieldName());
		}
		if (input.getStatFieldName() != null) {
			this.wStatFieldName.setText(input.getStatFieldName());
		}
		if (input.getStatCnd() != null) {
			this.wStatCnd.setText(input.getStatCnd());
		}
		if (input.getBatchSize() != null) {
			this.wBatchSize.setText(input.getBatchSize());
		}
        
        enableFieldName();
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
        this.input.setOutputType(wOutputType.getText());
        this.input.setUrl(wUrl.getText());
        this.input.setClusterName(wClusterName.getText());
        this.input.setDatabase(wDatabase.getText());
        this.input.setOutputTable(wTable.getText());
        this.input.setFieldName(wFieldName.getText());
        this.input.setStatFieldName(wStatFieldName.getText());
        this.input.setStatCnd(wStatCnd.getText());
        this.input.setBatchSize(wBatchSize.getText());
        
        if(!createIndex()){
        	MessageBox mb = new MessageBox(shell);
        	mb.setMessage("ES地址似乎不正确，请检查！");
        	mb.open();
        }

        dispose();
    }

}