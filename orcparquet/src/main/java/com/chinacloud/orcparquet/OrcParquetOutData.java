package com.chinacloud.orcparquet;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class OrcParquetOutData extends BaseStepData implements StepDataInterface {
    protected RowMetaInterface outputRowMeta;
    public RowMetaInterface inputRowMeta;


    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    public void setOutputRowMeta(RowMetaInterface rmi) {
        outputRowMeta = rmi;
    }

    public OrcParquetOutData() {

    }


}