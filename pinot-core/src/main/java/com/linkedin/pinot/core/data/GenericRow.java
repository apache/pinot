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
package com.linkedin.pinot.core.data;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.pinot.common.data.RowEvent;


/**
 * A plain implementation of RowEvent based on HashMap.
 *
 *
 */
public class GenericRow implements RowEvent {
  private Map<String, Object> _fieldMap = new HashMap<String, Object>();
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void init(Map<String, Object> field) {
    _fieldMap = field;
  }

  @Override
  public String[] getFieldNames() {
    return _fieldMap.keySet().toArray(new String[_fieldMap.size()]);
  }

  @Override
  public Object getValue(String fieldName) {
    return _fieldMap.get(fieldName);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for (String key : _fieldMap.keySet()) {
      if (_fieldMap.get(key) instanceof Object[]) {
        b.append(key + " : " + Arrays.toString((Object[]) _fieldMap.get(key)) + ", ");
      } else {
        b.append(key + " : " + _fieldMap.get(key) + ", ");
      }

    }

    return b.toString();
  }

  @Override
  public int hashCode() {
    return _fieldMap.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GenericRow)) {
      return false;
    }
    GenericRow r = (GenericRow) o;
    return _fieldMap.equals(r._fieldMap);
  }

static TypeReference typeReference = new TypeReference<Map<String, Object>>() {
};
  public static GenericRow fromBytes(byte[] buffer) throws IOException {
    Map<String, Object> fieldMap = (Map<String, Object>) OBJECT_MAPPER.readValue(buffer, typeReference);
    GenericRow genericRow = new GenericRow();
    genericRow.init(fieldMap);
    return genericRow;
  }

  public byte[] toBytes() throws IOException {
    StringWriter writer = new StringWriter();
    OBJECT_MAPPER.writeValue(writer, _fieldMap);
    return writer.toString().getBytes(Charset.forName("UTF-8"));
  }
}
