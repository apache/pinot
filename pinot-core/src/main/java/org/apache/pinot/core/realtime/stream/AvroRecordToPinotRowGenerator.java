/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.realtime.stream;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.TreeSet;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.RecordReaderUtils;


public class AvroRecordToPinotRowGenerator {

  private final Schema _schema;
  private final FieldSpec _incomingTimeFieldSpec;

  public AvroRecordToPinotRowGenerator(Schema schema) {
    _schema = schema;

    // For time field, we use the incoming time field spec
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    Preconditions.checkNotNull(timeFieldSpec);
    _incomingTimeFieldSpec = new TimeFieldSpec(timeFieldSpec.getIncomingGranularitySpec());
  }

  public GenericRow transform(GenericData.Record from, GenericRow to) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      FieldSpec incomingFieldSpec =
          fieldSpec.getFieldType() == FieldSpec.FieldType.TIME ? _incomingTimeFieldSpec : fieldSpec;
      String fieldName = incomingFieldSpec.getName();
      //Handle MAP types
      if (fieldName.toUpperCase().endsWith("__KEYS")) {
        String avroFieldName = fieldName.replaceAll("__KEYS", "");
        Object o = from.get(avroFieldName);
        if (o instanceof Map) {
          Map map = (Map) o;
          TreeSet sortedKeySet = new TreeSet(map.keySet());
          Object[] keys = new Object[map.size()];
          int i = 0;
          for (Object key : sortedKeySet) {
            keys[i++] = RecordReaderUtils.convert(incomingFieldSpec, key);
          }
          to.putField(fieldName, keys);
        }
      } else if (fieldName.toUpperCase().endsWith("__VALUES")) {
        String avroFieldName = fieldName.replaceAll("__VALUES", "");
        Object o = from.get(avroFieldName);
        if (o instanceof Map) {
          Map map = (Map) o;
          TreeSet sortedKeySet = new TreeSet(map.keySet());
          Object[] values = new Object[map.size()];
          int i = 0;
          for (Object key : sortedKeySet) {
            values[i++] = RecordReaderUtils.convert(incomingFieldSpec, map.get(key));
          }
          to.putField(fieldName, values);
        }
      } else {
        to.putField(fieldName, RecordReaderUtils.convert(incomingFieldSpec, from.get(fieldName)));
      }
    }
    return to;
  }
}
