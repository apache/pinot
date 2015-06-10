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
package com.linkedin.pinot.core.realtime.impl.kafka;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;


public class KafkaJSONMessageDecoder implements KafkaMessageDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJSONMessageDecoder.class);

  private Schema schema;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String kafkaTopicName) throws Exception {
    this.schema = indexingSchema;
  }

  @Override
  public GenericRow decode(byte[] payload) {
    try {
      String text = new String(payload, "UTF-8");
      JSONObject message = new JSONObject(text);
      Map<String, Object> rowEntries = new HashMap<String, Object>();
      for (FieldSpec dimension : schema.getDimensionFieldSpecs()) {
        if (message.has(dimension.getName())) {
          Object entry;
          if (dimension.isSingleValueField()) {
            entry = stringToDataType(dimension.getDataType(), message.getString(dimension.getName()));
          } else {
            JSONArray jsonArray = message.getJSONArray(dimension.getName());
            Object[] array = new Object[jsonArray.length()];
            for (int i = 0; i < array.length; i++) {
              array[i] = stringToDataType(dimension.getDataType(), jsonArray.getString(i));
            }
            entry = array;
          }
          rowEntries.put(dimension.getName(), entry);
        } else {
          Object entry = AvroRecordReader.getDefaultNullValue(dimension);
          rowEntries.put(dimension.getName(), entry);
        }
      }

      for (FieldSpec metric : schema.getMetricFieldSpecs()) {
        if (message.has(metric.getName())) {
          Object entry = stringToDataType(metric.getDataType(), message.getString(metric.getName()));
          rowEntries.put(metric.getName(), entry);
        } else {
          Object entry = AvroRecordReader.getDefaultNullValue(metric);
          rowEntries.put(metric.getName(), entry);
        }
      }

      TimeFieldSpec spec = schema.getTimeFieldSpec();
      if (message.has(spec.getName())) {
        Object entry = stringToDataType(spec.getDataType(), message.getString(spec.getName()));
        rowEntries.put(spec.getName(), entry);
      } else {
        Object entry = AvroRecordReader.getDefaultNullValue(spec);
        rowEntries.put(spec.getName(), entry);
      }

      GenericRow row = new GenericRow();
      row.init(rowEntries);
      return row;
    } catch (Exception e) {
      LOGGER.error("error decoding , ", e);
    }
    return null;
  }

  private Object stringToDataType(DataType type, String inString) {
    if (inString == null) {
      return null;
    }

    switch (type) {
      case INT:
        return new Integer(Integer.parseInt(inString));
      case LONG:
        return new Long(Long.parseLong(inString));
      case FLOAT:
        return new Float(Float.parseFloat(inString));
      case DOUBLE:
        return new Double(Double.parseDouble(inString));
      case BOOLEAN:
      case STRING:
        return inString.toString();
      default:
        return null;
    }
  }
}
