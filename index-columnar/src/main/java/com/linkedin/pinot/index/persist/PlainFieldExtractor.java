package com.linkedin.pinot.index.persist;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;


/**
 * This implementation will only inject columns inside the Schema.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class PlainFieldExtractor implements FieldExtractor {

  Schema _schema = null;

  PlainFieldExtractor(Schema schema) {
    _schema = schema;
  }

  @Override
  public void setSchema(Schema schema) {
    _schema = schema;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow transform(GenericRow row) {
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    if (_schema.size() > 0) {
      for (String column : _schema.getColumnNames()) {
        fieldMap.put(column, row.getValue(column));
      }
      row.init(fieldMap);
    }
    return row;
  }

}
