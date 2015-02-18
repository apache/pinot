package com.linkedin.pinot.core.indexsegment.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Maps;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class AvroUtils {

  /**
   * gives back a basic pinot schema object with field type as unknown and not aware of whether SV or MV
   * this is just a util method for testing
   * @param avroFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static Schema extractSchemaFromAvro(File avroFile) throws FileNotFoundException, IOException {
    final Schema schema = new Schema();
    final DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile);
    final org.apache.avro.Schema avroSchema = dataStreamReader.getSchema();
    for (final Field field : avroSchema.getFields()) {
      final FieldSpec spec = new DimensionFieldSpec();
      spec.setName(field.name());
      spec.setDataType(AvroRecordReader.getColumnType(field));
      if (field.name().contains("count") || field.name().contains("met")) {
        spec.setFieldType(FieldType.metric);
      } else if (field.name().contains("day") || field.name().equals("daysSinceEpoch")) {
        spec.setFieldType(FieldType.time);
      } else {
        spec.setFieldType(FieldType.dimension);
      }
      schema.addSchema(spec.getName(), spec);
    }
    dataStreamReader.close();
    return schema;
  }

  public static List<String> getAllColumnsInAvroFile(File avroFile) throws FileNotFoundException, IOException {
    final List<String> ret = new ArrayList<String>();
    final DataFileStream<GenericRecord> reader = getAvroReader(avroFile);
    for (final Field f : reader.getSchema().getFields()) {
      ret.add(f.name());
    }
    reader.close();
    return ret;
  }

  public static DataFileStream<GenericRecord> getAvroReader(File avroFile) throws FileNotFoundException, IOException {
    return new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
  }

}
