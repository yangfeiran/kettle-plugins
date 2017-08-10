package com.chinacloud.incetl.trans.step;


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
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import com.chinacloud.incetl.Constant;

public class IncETLInputDialog extends BaseStepDialog
        implements StepDialogInterface {
    private static Class<?> PKG = IncETLInputMeta.class;
    private IncETLInputMeta input;

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

    
	private ModifyListener lsMod;
	private Button wTestConnection;

    public IncETLInputDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.input = ((IncETLInputMeta) in);
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
                IncETLInputDialog.this.input.setChanged();
            }
        };
        this.changed = this.input.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        
        this.shell.setLayout(formLayout);

        this.shell.setText("转换步骤增量信息配置");

        int middle = this.props.getMiddlePct();
        middle -= 10;
        int margin = 6;

        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
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
        this.wlIncType = new Label(this.shell, 131072);
        this.wlIncType.setText("增量类型");
        this.props.setLook(this.wlIncType);
        this.fdlIncType = new FormData();
        this.fdlIncType.left = new FormAttachment(0, 0);
        this.fdlIncType.right = new FormAttachment(middle, -margin);
        this.fdlIncType.top = new FormAttachment(this.wStepname, margin);
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
        this.fdIncType.top = new FormAttachment(this.wStepname, margin);
        this.wIncType.setLayoutData(this.fdIncType);
        this.wIncType.addModifyListener(new ModifyListener() {
        	public void modifyText( ModifyEvent e ) {
                input.setChanged();
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
        

        // For Button
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));
        

        BaseStepDialog.positionBottomButtons(this.shell,
        		new Button[]{wOK, wCancel}, margin, this.wEndValue);

        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                IncETLInputDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                IncETLInputDialog.this.ok();
            }
        };

        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);


        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                IncETLInputDialog.this.shell.dispose();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                IncETLInputDialog.this.shell.dispose();
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

    protected void enableDataFormat() {
    	boolean flag = "lastmodify".equals(Constant.incType.get(wIncType.getText()));
		wlDataFormat.setEnabled(flag);
        wDataFormat.setEnabled(flag);
	}

	

	public void getData() {
        this.wStepname.selectAll();
        this.wIncType.setText(input.getIncType());
        this.wIncField.setText(input.getIncField());
        this.wDataFormat.setText(input.getDataFormat());
        this.wStartValue.setText(input.getStartValue());
        this.wEndValue.setText(input.getEndValue());
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
        this.input.setIncType(wIncType.getText());
        this.input.setIncField(wIncField.getText());
        this.input.setDataFormat(wDataFormat.getText());
        this.input.setStartValue(wStartValue.getText());
        this.input.setEndValue(wEndValue.getText());

        dispose();
    }

}