/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.stream.StreamMessageDecoder;
import java.util.Arrays;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaJSONMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJSONMessageDecoder.class);

  private Schema schema;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
    this.schema = indexingSchema;
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      String text = new String(payload, "UTF-8");
      JSONObject message = new JSONObject(text);

      for (FieldSpec dimensionSpec : schema.getDimensionFieldSpecs()) {
        readFieldValue(destination, message, dimensionSpec);
      }

      for (FieldSpec metricSpec : schema.getMetricFieldSpecs()) {
        readFieldValue(destination, message, metricSpec);
      }

      TimeFieldSpec timeSpec = schema.getTimeFieldSpec();
      readFieldValue(destination, message, timeSpec);

      return destination;
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row.", e);
      return null;
    }
  }

  private void readFieldValue(GenericRow destination, JSONObject message, FieldSpec dimensionSpec)
      throws JSONException {
    String columnName = dimensionSpec.getName();
    if (message.has(columnName) && !message.isNull(columnName)) {
      Object entry;
      if (dimensionSpec.isSingleValueField()) {
        entry = stringToDataType(dimensionSpec, message.get(columnName).toString());
      } else {
        JSONArray jsonArray = message.getJSONArray(columnName);
        Object[] array = new Object[jsonArray.length()];
        for (int i = 0; i < array.length; i++) {
          array[i] = stringToDataType(dimensionSpec, jsonArray.getString(i));
        }
        if (array.length == 0) {
          entry = new Object[]{dimensionSpec.getDefaultNullValue()};
        } else {
          entry = array;
        }
      }
      destination.putField(columnName, entry);
    } else {
      Object entry = dimensionSpec.getDefaultNullValue();
      destination.putField(columnName, entry);
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  private Object stringToDataType(FieldSpec spec, String inString) {
    if (inString == null) {
      return spec.getDefaultNullValue();
    }

    try {
      switch (spec.getDataType()) {
        case INT:
          return Integer.parseInt(inString);
        case LONG:
          return Long.parseLong(inString);
        case FLOAT:
          return Float.parseFloat(inString);
        case DOUBLE:
          return Double.parseDouble(inString);
        case BOOLEAN:
        case STRING:
          return inString;
        default:
          return null;
      }
    } catch (NumberFormatException e) {
      Object nullValue = spec.getDefaultNullValue();
      LOGGER.warn("Failed to parse {} as a value of type {} for column {}, defaulting to {}", inString,
          spec.getDataType(), spec.getName(), nullValue, e);
      return nullValue;
    }
  }
}
