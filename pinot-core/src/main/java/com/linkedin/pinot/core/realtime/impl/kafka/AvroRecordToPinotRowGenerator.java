package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.util.Utf8;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;


public class AvroRecordToPinotRowGenerator {
  private final Schema indexingSchema;

  public AvroRecordToPinotRowGenerator(Schema indexingSchema) {
    this.indexingSchema = indexingSchema;
  }

  public GenericRow transform(GenericData.Record record) {
    Map<String, Object> rowEntries = new HashMap<String, Object>();
    for (String column : indexingSchema.getColumnNames()) {
      Object entry = record.get(column);
      if (entry instanceof Utf8) {
        entry = ((Utf8) entry).toString();
      }
      if (entry instanceof Array) {
        entry = AvroRecordReader.transformAvroArrayToObjectArray((Array) entry);
      }
      rowEntries.put(column, entry);
    }

    GenericRow row = new GenericRow();
    row.init(rowEntries);
    return row;
  }
}
