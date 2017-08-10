package com.chinacloud.hbase.output;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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
import org.eclipse.swt.layout.GridData;
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
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.util.HelpUtils;

public class HBaseOutputDialog extends BaseStepDialog
        implements StepDialogInterface {
    private static Class<?> PKG = HBaseOutputDialog.class;
    private HBaseOutputMeta input;

    private Label wlZKHostPorts;
    private Text wZKHostPorts;
    private FormData fdlZKHostPorts, fdZKHostPorts;
    
    private Label wlHBaseMaster;
    private Text wHBaseMaster;
    private FormData fdlHBaseMaster, fdHBaseMaster;
    
    private Label wlHBaseRootdir;
    private Text wHBaseRootdir;
    private FormData fdlHBaseRootdir, fdHBaseRootdir;
    
    private Label wlTableName;
    private Text wTableName;
    private FormData fdlTableName, fdTableName;
    
    private Label wlFamilyName;
    private Text wFamilyName;
    private FormData fdlFamilyName, fdFamilyName;
    
    private Label wlBatchSize;
    private Text wBatchSize;
    private FormData fdlBatchSize, fdBatchSize;
    
    public HBaseOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.input = ((HBaseOutputMeta) in);
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);
        this.shell.setSize(200, 300);
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
                HBaseOutputDialog.this.input.setChanged();
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
        
        //For wZKHostPorts
        this.wlZKHostPorts = new Label(this.shell, 131072);
        this.wlZKHostPorts.setText("ZooKeeper地址");
        this.props.setLook(this.wlZKHostPorts);
        this.fdlZKHostPorts = new FormData();
        this.fdlZKHostPorts.left = new FormAttachment(0, 0);
        this.fdlZKHostPorts.right = new FormAttachment(middle, -margin);
        this.fdlZKHostPorts.top = new FormAttachment(this.wStepname, margin);//距上一个控件的位置
        this.wlZKHostPorts.setLayoutData(this.fdlZKHostPorts);
        
        this.wZKHostPorts = new Text(this.shell, 18436);
        this.props.setLook(this.wZKHostPorts);
        this.wZKHostPorts.addModifyListener(lsMod);
        this.fdZKHostPorts = new FormData();
        this.fdZKHostPorts.left = new FormAttachment(middle, 0);
        this.fdZKHostPorts.right = new FormAttachment(100, 0);
        this.fdZKHostPorts.top = new FormAttachment(this.wStepname, margin);//距上一个控件的位置
        this.wZKHostPorts.setLayoutData(this.fdZKHostPorts);
        
        //For wHBaseMaster
        this.wlHBaseMaster = new Label(this.shell, 131072);
        this.wlHBaseMaster.setText("HBase Master");
        this.props.setLook(this.wlHBaseMaster);
        this.fdlHBaseMaster = new FormData();
        this.fdlHBaseMaster.left = new FormAttachment(0, 0);
        this.fdlHBaseMaster.right = new FormAttachment(middle, -margin);
        this.fdlHBaseMaster.top = new FormAttachment(this.wZKHostPorts, margin);//距上一个控件的位置
        this.wlHBaseMaster.setLayoutData(this.fdlHBaseMaster);
        
        this.wHBaseMaster = new Text(this.shell, 18436);
        this.props.setLook(this.wHBaseMaster);
        this.wHBaseMaster.addModifyListener(lsMod);
        this.fdHBaseMaster = new FormData();
        this.fdHBaseMaster.left = new FormAttachment(middle, 0);
        this.fdHBaseMaster.right = new FormAttachment(100, 0);
        this.fdHBaseMaster.top = new FormAttachment(this.wZKHostPorts, margin);//距上一个控件的位置
        this.wHBaseMaster.setLayoutData(this.fdHBaseMaster);
        
        //For HBaseRootdir
        this.wlHBaseRootdir = new Label(this.shell, SWT.RIGHT);
        this.wlHBaseRootdir.setText("HBase Rootdir");
        this.props.setLook(this.wlHBaseRootdir);
        this.fdlHBaseRootdir = new FormData();
        this.fdlHBaseRootdir.left = new FormAttachment(0, 0);
        this.fdlHBaseRootdir.right = new FormAttachment(middle, -margin);
        this.fdlHBaseRootdir.top = new FormAttachment(this.wHBaseMaster, margin);
        this.wlHBaseRootdir.setLayoutData(this.fdlHBaseRootdir);
        
        this.wHBaseRootdir = new Text(this.shell, 18436);
        this.props.setLook(this.wHBaseRootdir);
        this.wHBaseRootdir.addModifyListener(lsMod);
        this.fdHBaseRootdir = new FormData();
        this.fdHBaseRootdir.left = new FormAttachment(middle, 0);
        this.fdHBaseRootdir.right = new FormAttachment(100, 0);
        this.fdHBaseRootdir.top = new FormAttachment(this.wHBaseMaster, margin);
        this.wHBaseRootdir.setLayoutData(this.fdHBaseRootdir);
        
        //HBase表名
        this.wlTableName = new Label(this.shell, SWT.RIGHT);
        this.wlTableName.setText("HBase表名");
        this.props.setLook(this.wlTableName);
        this.fdlTableName = new FormData();
        this.fdlTableName.left = new FormAttachment(0, 0);
        this.fdlTableName.right = new FormAttachment(middle, -margin);
        this.fdlTableName.top = new FormAttachment(this.wHBaseRootdir, margin);
        this.wlTableName.setLayoutData(this.fdlTableName);
        
        this.wTableName = new Text(this.shell, 18436);
        this.props.setLook(this.wTableName);
        this.wTableName.addModifyListener(lsMod);
        this.fdTableName = new FormData();
        this.fdTableName.left = new FormAttachment(middle, 0);
        this.fdTableName.right = new FormAttachment(100, 0);
        this.fdTableName.top = new FormAttachment(this.wHBaseRootdir, margin);
        this.wTableName.setLayoutData(this.fdTableName);
        
        //FamilyName
        this.wlFamilyName = new Label(this.shell, SWT.RIGHT);
        this.wlFamilyName.setText("Family Name");
        this.props.setLook(this.wlFamilyName);
        this.fdlFamilyName = new FormData();
        this.fdlFamilyName.left = new FormAttachment(0, 0);
        this.fdlFamilyName.right = new FormAttachment(middle, -margin);
        this.fdlFamilyName.top = new FormAttachment(this.wTableName, margin);
        this.wlFamilyName.setLayoutData(this.fdlFamilyName);
        
        this.wFamilyName = new Text(this.shell, SWT.BORDER);
        this.props.setLook(this.wFamilyName);
        this.wFamilyName.addModifyListener(lsMod);
        this.fdFamilyName = new FormData();
        this.fdFamilyName.left = new FormAttachment(middle, 0);
        this.fdFamilyName.right = new FormAttachment(100, 0);
        this.fdFamilyName.top = new FormAttachment(this.wTableName, margin);
        this.wFamilyName.setLayoutData(this.fdFamilyName);
        
        //For BatchSize
        this.wlBatchSize = new Label(this.shell, 131072);
        this.wlBatchSize.setText("提交批量(条)");
        this.props.setLook(this.wlBatchSize);
        this.fdlBatchSize = new FormData();
        this.fdlBatchSize.left = new FormAttachment(0, 0);
        this.fdlBatchSize.right = new FormAttachment(middle, -margin);
        this.fdlBatchSize.top = new FormAttachment(this.wFamilyName, margin);//距上一个控件的位置
        this.wlBatchSize.setLayoutData(this.fdlBatchSize);
        
        this.wBatchSize = new Text(this.shell, 18436);
        this.props.setLook(this.wBatchSize);
        this.wBatchSize.addModifyListener(lsMod);
        this.fdBatchSize = new FormData();
        this.fdBatchSize.left = new FormAttachment(middle, 0);
        this.fdBatchSize.right = new FormAttachment(100, 0);
        this.fdBatchSize.top = new FormAttachment(this.wFamilyName, margin);//距上一个控件的位置
        this.wBatchSize.setLayoutData(this.fdBatchSize);
        

        // For Button
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        

        BaseStepDialog.positionBottomButtons(this.shell,
        		new Button[]{wOK, wCancel}, margin, wBatchSize);

        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                HBaseOutputDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                HBaseOutputDialog.this.ok();
            }
        };
        

        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                HBaseOutputDialog.this.shell.dispose();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                HBaseOutputDialog.this.shell.dispose();
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


	public void getData() {
        this.wStepname.selectAll();
        this.wZKHostPorts.setText(this.input.getZkHostPorts());
        this.wHBaseMaster.setText(this.input.getHbaseMaster());
        this.wHBaseRootdir.setText(input.getHbaseRootdir());
        this.wTableName.setText(input.getTableName());
        this.wFamilyName.setText(input.getFamilyName()); 
        if (input.getBatchSize() != null) {
			this.wBatchSize.setText(input.getBatchSize());
		}
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
        this.input.setZkHostPorts(wZKHostPorts.getText());
        this.input.setHbaseMaster(wHBaseMaster.getText());
        this.input.setHbaseRootdir(wHBaseRootdir.getText());
        this.input.setTableName(wTableName.getText());
        this.input.setFamilyName(wFamilyName.getText());
        this.input.setBatchSize(wBatchSize.getText());

        dispose();
    }

    /*@Override
    protected Button createHelpButton( Shell shell, StepMeta stepMeta, PluginInterface plugin ) {
      return HelpUtils.createHelpButton( helpComp, HelpUtils.getHelpDialogTitle( plugin ), plugin );
    }*/
}