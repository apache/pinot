/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
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
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      String text = new String(payload, "UTF-8");
      JSONObject message = new JSONObject(text);
      for (FieldSpec dimensionSpec : schema.getDimensionFieldSpecs()) {
        if (message.has(dimensionSpec.getName())) {
          Object entry;
          if (dimensionSpec.isSingleValueField()) {
            entry = stringToDataType(dimensionSpec, message.getString(dimensionSpec.getName()));
          } else {
            JSONArray jsonArray = message.getJSONArray(dimensionSpec.getName());
            Object[] array = new Object[jsonArray.length()];
            for (int i = 0; i < array.length; i++) {
              array[i] = stringToDataType(dimensionSpec, jsonArray.getString(i));
            }
            if (array.length == 0) {
              entry = new Object[] { AvroRecordReader.getDefaultNullValue(dimensionSpec) };
            } else {
              entry = array;
            }
          }
          destination.putField(dimensionSpec.getName(), entry);
        } else {
          Object entry = AvroRecordReader.getDefaultNullValue(dimensionSpec);
          destination.putField(dimensionSpec.getName(), entry);
        }
      }

      for (FieldSpec metricSpec : schema.getMetricFieldSpecs()) {
        if (message.has(metricSpec.getName())) {
          Object entry = stringToDataType(metricSpec, message.getString(metricSpec.getName()));
          destination.putField(metricSpec.getName(), entry);
        } else {
          Object entry = AvroRecordReader.getDefaultNullValue(metricSpec);
          destination.putField(metricSpec.getName(), entry);
        }
      }

      TimeFieldSpec timeSpec = schema.getTimeFieldSpec();
      if (message.has(timeSpec.getName())) {
        Object entry = stringToDataType(timeSpec, message.getString(timeSpec.getName()));
        destination.putField(timeSpec.getName(), entry);
      } else {
        Object entry = AvroRecordReader.getDefaultNullValue(timeSpec);
        destination.putField(timeSpec.getName(), entry);
      }

      return destination;
    } catch (Exception e) {
      LOGGER.error("error decoding , ", e);
    }
    return null;
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  private Object stringToDataType(FieldSpec spec, String inString) {
    if (inString == null) {
      return AvroRecordReader.getDefaultNullValue(spec);
    }

    switch (spec.getDataType()) {
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
