package org.apache.pinot.core.segment.processing.utils;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentProcessingUtilsTest {
  private Schema _avroSchema;
  private org.apache.pinot.spi.data.Schema _pinotSchema;

  @Test
  public void  pinotSchemaToAvroSchemaConversionTest(){
    _pinotSchema = new org.apache.pinot.spi.data.Schema.SchemaBuilder().setSchemaName("mySchema")
        .addSingleValueDimension("nullableIntField", DataType.INT).build();
    _avroSchema = SegmentProcessorUtils.convertPinotSchemaToAvroSchema(_pinotSchema);
    final Field field = _avroSchema.getField("nullableIntField");
    Assert.assertEquals(field.schema().getType(), Type.UNION);
    Assert.assertEquals(field.schema().getTypes(), Lists.newArrayList(Type.NULL, Type.INT));
    GenericRecord record  = new GenericRecordBuilder(_avroSchema).set("nullableIntField", Integer.valueOf(0)).build();
  }

}
