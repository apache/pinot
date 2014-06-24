package com.linkedin.pinot.index.persist;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.index.data.Schema;


public class FieldExtractorProvider {

  private static String COMMA = ",";
  private static String DATA_SCHEMA_PROJECTED_COLUMN = "data.schema.projected.column";
  private static String DATA_SCHEMA = "data.schema";
  private static String DOT = ".";
  private static String DELIMETER = "delimeter";
  private static String FIELD_TYPE = "field.type";
  private static String UNKNOWN = "unknown";

  public static FieldExtractor get(final Configuration dataReaderSpec) {
    return new PlainFieldExtractor(getSchema(dataReaderSpec));
  }

  private static Schema getSchema(final Configuration dataReaderSpec) {
    Schema schema = new Schema();
    List<String> projectedColumnList = dataReaderSpec.getList(DATA_SCHEMA_PROJECTED_COLUMN, new ArrayList<String>());
    for (String column : projectedColumnList) {
      FieldSpec fieldSpec = new FieldSpec();
      fieldSpec.setFieldType(FieldType.valueOf(dataReaderSpec.getString(DATA_SCHEMA + DOT + column + DOT + FIELD_TYPE,
          UNKNOWN)));
      fieldSpec.setDelimeter(dataReaderSpec.getString(DATA_SCHEMA + DOT + column + DOT + DELIMETER, COMMA));
      schema.addSchema(column, fieldSpec);
    }
    return schema;
  }
}
