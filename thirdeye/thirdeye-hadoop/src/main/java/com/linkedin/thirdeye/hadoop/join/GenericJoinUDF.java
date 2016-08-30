package com.linkedin.thirdeye.hadoop.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.hadoop.join.GenericJoinUDFConfig.Field;

public class GenericJoinUDF implements JoinUDF {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericJoinUDF.class);
  private GenericJoinUDFConfig config;
  private Schema outputSchema;
  private List<Field> fields;

  public GenericJoinUDF(Map<String, String> params) {
    LOGGER.info("Initializing GenericJoinUDF with params:" + params);
    this.config = new GenericJoinUDFConfig(params);
    fields = config.getFields();
  }

  @Override
  public void init(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }

  /**
   * Trivial implementation of a generic join udf. Assumes the data type is the
   * same in source and output.
   */
  @Override
  public List<GenericRecord> performJoin(Object joinKeyVal,
      Map<String, List<GenericRecord>> joinInput) {

    List<GenericRecord> outputRecords = new ArrayList<GenericRecord>();
    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    for (Field field : fields) {
      Object value = null;
      // try to find the field in one of the source events, break out as soon as
      // we find a non null value
      for (String source : field.sourceEvents) {
        List<GenericRecord> list = joinInput.get(source);
        if (list != null && list.size() >= 1) {
          for (GenericRecord record : list) {
            value = record.get(field.name);
            if (value != null) {
              break;
            }
          }
        }
        if (value != null) {
          break;
        }
      }
      if (value != null) {
        outputRecord.put(field.name, value);
      }
    }
    outputRecords.add(outputRecord);
    return outputRecords;
  }

}
