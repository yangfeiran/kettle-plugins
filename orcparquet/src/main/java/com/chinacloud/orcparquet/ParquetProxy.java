package com.chinacloud.orcparquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;

import com.chinacloud.orcparquet.util.ExampleParquetWriter;

public class ParquetProxy implements IProxy{
	private Configuration conf;
	private Path path;
	private ParquetWriter<Group> writer = null;
	private MessageType schema = null;
	private SimpleGroupFactory f;
	private Mode writeMode = Mode.CREATE;
	private int blockSize = 134217728; //块大小:128M
	private int pageSize = 1048576; //页大小：1024K

	public ParquetProxy(Configuration conf, String fileName, MessageType schema, 
			SimpleGroupFactory f, boolean cleanOutput) throws KettleFileException {
		path = new Path(fileName);
		this.conf = conf;
		this.schema = schema;
		this.f = f;
		FileSystem fs;
		try {
			fs = path.getFileSystem(conf);
			if (!fs.exists(path.getParent())) {
			    fs.mkdirs(path.getParent(), FsPermission.getDirDefault());
			}
			fs.setPermission(path.getParent(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
			if (cleanOutput && fs.exists(path)) {
				fs.delete(path, true);
				writeMode = Mode.OVERWRITE;
		    }else{
		    	writeMode = Mode.CREATE;
		    }
		} catch (Exception e) {
			e.printStackTrace();
			throw new KettleFileException(e.getCause());
		}
	}
	
	@Override
	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}
	
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	
	@Override
	public void start() throws KettleException, IOException {
		if(schema == null){
			throw new KettleException("file schema is not set");
		}
		writer = ExampleParquetWriter.builder(path)
				.withConf(conf)
		        .withType(schema)
		        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
		        .withWriteMode(writeMode)
		        .withRowGroupSize(blockSize)
		        .withPageSize(pageSize)
		        .build();
	}
	
	public void writeRow(Group group) throws IOException {
		writer.write(group);
	}
	
	@Override
	public void writeRow(Object[] row, RowMetaInterface rowMeta, String[] fields) throws IOException {
		Group group = f.newGroup();
    	for(int i=0;i<fields.length;i++){
    		switch (rowMeta.getValueMeta(i).getType()){
    	    case 5:
    	    	group.add(fields[i], row[i]!=null ? (long)row[i] : 0);
    	      break;
    	    case 1:
    	    case 6:
    	    	group.add(fields[i], row[i]!=null ? Double.valueOf(row[i].toString()) : 0);
    	      break;
    	    case 4:
    	    	group.add(fields[i], row[i]!=null ? (boolean)row[i] : false);
    	      break;
    	    case 2:
    	    case 3:
    	    default:
    	    	group.add(fields[i], row[i]!=null ? row[i].toString() : "null");
    	    }
    	}
    	writer.write(group);
	}
	
	@Override
	public void close() throws IOException {
		if(writer != null){
			writer.close();
			path.getFileSystem(conf)
				.setPermission(path, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
		}
	}
}
