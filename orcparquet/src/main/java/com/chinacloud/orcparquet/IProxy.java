package com.chinacloud.orcparquet;

import java.io.IOException;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;

public interface IProxy {
	public void setBlockSize(int blockSize);
	public void setPageSize(int parseInt);
	public void start() throws IOException, KettleException;
	public void writeRow(Object[] row, RowMetaInterface rowMeta, String[] fields) throws IOException;
	public void close() throws IOException;
}
