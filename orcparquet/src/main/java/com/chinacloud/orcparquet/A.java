package com.chinacloud.orcparquet;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

public class A {

	public static void main(String[] args) {
		 MessageType schema = Types.buildMessage()
     		      .required(INT32).named("id")
     		      .required(PrimitiveTypeName.DOUBLE).named("id2")
     		      .required(PrimitiveTypeName.INT64).named("id3")
     		      .required(PrimitiveTypeName.BOOLEAN).named("id4")
     		      .required(PrimitiveTypeName.INT96).named("id5")
   		      .required(BINARY).as(UTF8).named("name")
   		      .named("dataSchema");
		 System.out.println(schema);
		 
		 TypeDescription schema2 = TypeDescription.createStruct()
					.addField("field1", TypeDescription.createString())
			        .addField("field2", TypeDescription.createLong())
			        .addField("field3", TypeDescription.createString());
		 System.out.println(schema2);
	}

}
