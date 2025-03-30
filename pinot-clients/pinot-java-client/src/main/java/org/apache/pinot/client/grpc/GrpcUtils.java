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
package org.apache.pinot.client.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.common.compression.CompressionFactory;
import org.apache.pinot.common.compression.Compressor;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.encoder.ResponseEncoder;
import org.apache.pinot.common.response.encoder.ResponseEncoderFactory;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;


public class GrpcUtils {

  private GrpcUtils() {
  }

  public static ObjectNode extractMetadataJson(Broker.BrokerResponse brokerResponse)
      throws IOException {
    ObjectNode metadata = JsonUtils.newObjectNode();
    JsonNode jsonNode = JsonUtils.bytesToJsonNode(brokerResponse.getPayload().toByteArray());
    Iterator<String> fieldNamesIterator = jsonNode.fieldNames();
    while (fieldNamesIterator.hasNext()) {
      String fieldName = fieldNamesIterator.next();
      metadata.set(fieldName, jsonNode.get(fieldName));
    }
    metadata.set("metadataMap", JsonUtils.objectToJsonNode(brokerResponse.getMetadataMap()));
    return metadata;
  }

  public static DataSchema extractSchema(Broker.BrokerResponse brokerResponse)
      throws IOException {
    return DataSchema.fromBytes(brokerResponse.getPayload().asReadOnlyByteBuffer());
  }

  public static JsonNode extractSchemaJson(Broker.BrokerResponse brokerResponse)
      throws IOException {
    return JsonUtils.objectToJsonNode(extractSchema(brokerResponse));
  }

  public static ResultTable extractResultTable(Broker.BrokerResponse brokerResponse, DataSchema schema)
      throws IOException {
    Map<String, String> metadataMap = brokerResponse.getMetadataMap();
    String compressionAlgorithm = metadataMap.getOrDefault(CommonConstants.Broker.Grpc.COMPRESSION,
        CommonConstants.Broker.Grpc.DEFAULT_COMPRESSION);
    Compressor compressor = CompressionFactory.getCompressor(compressionAlgorithm);
    String encodingType = metadataMap.getOrDefault(CommonConstants.Broker.Grpc.ENCODING,
        CommonConstants.Broker.Grpc.DEFAULT_ENCODING);
    ResponseEncoder responseEncoder = ResponseEncoderFactory.getResponseEncoder(encodingType);

    byte[] respBytes = brokerResponse.getPayload().toByteArray();
    int rowSize = Integer.parseInt(brokerResponse.getMetadataOrThrow("rowSize"));
    byte[] uncompressedPayload;
    try {
      uncompressedPayload = compressor.decompress(respBytes);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return responseEncoder.decodeResultTable(uncompressedPayload, rowSize, schema);
  }

  public static ExecutionStats extractExecutionStats(JsonNode executionStatsJson) {
    return ExecutionStats.fromJson(executionStatsJson);
  }
}
