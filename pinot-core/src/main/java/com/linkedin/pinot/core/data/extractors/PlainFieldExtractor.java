/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.extractors;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


/**
 * This implementation will only inject columns inside the Schema.
 *
 *
 */
public class PlainFieldExtractor implements FieldExtractor {

  Schema _schema = null;

  // Made public so it can be used in Pinot Admin code.
  public PlainFieldExtractor(Schema schema) {
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
