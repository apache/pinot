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
package org.apache.pinot.core.realtime.impl.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaJSONMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJSONMessageDecoder.class);

  private Schema _schema;
  private FieldSpec _incomingTimeFieldSpec;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName)
      throws Exception {
    _schema = indexingSchema;

    // For time field, we use the incoming time field spec
    TimeFieldSpec timeFieldSpec = _schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      _incomingTimeFieldSpec = new TimeFieldSpec(timeFieldSpec.getIncomingGranularitySpec());
    }
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload);
      for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
        FieldSpec incomingFieldSpec =
            fieldSpec.getFieldType() == FieldSpec.FieldType.TIME ? _incomingTimeFieldSpec : fieldSpec;
        String column = incomingFieldSpec.getName();
        destination.putValue(column, JsonUtils.extractValue(message.get(column), fieldSpec));
      }
      return destination;
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row.", e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
