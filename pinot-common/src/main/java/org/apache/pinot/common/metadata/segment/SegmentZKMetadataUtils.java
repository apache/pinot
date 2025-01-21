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
package org.apache.pinot.common.metadata.segment;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


public class SegmentZKMetadataUtils {
  private SegmentZKMetadataUtils() {
  }

  public static final ObjectMapper MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, true);
    mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, true);
    mapper.configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }

  public static String serialize(SegmentZKMetadata metadata)
      throws IOException {
    if (metadata == null) {
      return null;
    }
    return MAPPER.writeValueAsString(metadata.toZNRecord());
  }

  public static SegmentZKMetadata deserialize(String jsonString)
      throws IOException {
    if (jsonString == null || jsonString.isEmpty()) {
      return null;
    }
    ObjectNode objectNode = (ObjectNode) MAPPER.readTree(jsonString);
    ZNRecord znRecord = MAPPER.treeToValue(objectNode, ZNRecord.class);
    return new SegmentZKMetadata(znRecord);
  }

  public static SegmentZKMetadata deserialize(ObjectNode objectNode)
      throws IOException {
    if (objectNode == null) {
      return null;
    }
    ZNRecord znRecord = MAPPER.treeToValue(objectNode, ZNRecord.class);
    return new SegmentZKMetadata(znRecord);
  }

  public static SegmentZKMetadata deserialize(byte[] bytes)
      throws IOException {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    ZNRecord znRecord = MAPPER.readValue(bytes, ZNRecord.class);
    return new SegmentZKMetadata(znRecord);
  }
}
