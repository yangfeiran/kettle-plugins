package com.chinacloud.incetl.trans.step;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class IncETLInputData extends BaseStepData implements StepDataInterface {
    protected RowMetaInterface outputRowMeta;
    public RowMetaInterface inputRowMeta;


    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface rmi) {
        outputRowMeta = rmi;
    }

    public IncETLInputData() {

    }


}