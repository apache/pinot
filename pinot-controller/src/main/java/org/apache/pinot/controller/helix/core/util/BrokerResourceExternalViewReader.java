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
package org.apache.pinot.controller.helix.core.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads brokers external view from Zookeeper
 */
public class BrokerResourceExternalViewReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceExternalViewReader.class);
  private static final ObjectReader OBJECT_READER = new ObjectMapper().reader();
  public static final String BROKER_EXTERNAL_VIEW_PATH = "/EXTERNALVIEW/brokerResource";
  public static final String REALTIME_SUFFIX = "_REALTIME";
  public static final String OFFLINE_SUFFIX = "_OFFLINE";

  private ZkClient _zkClient;

  public BrokerResourceExternalViewReader(ZkClient zkClient) {
    _zkClient = zkClient;
  }

  protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
    return new ByteArrayInputStream(brokerResourceNodeData);
  }

  // Return a map from tablename (without type) to live brokers (of format host:port).
  public Map<String, List<String>> getTableToBrokersMap() {
    return getTableToBrokersMapFromExternalView(false, true);
  }

  // Return a map from tablename (with type) to live brokers (of raw instance format broker_host_port).
  public Map<String, List<String>> getTableWithTypeToRawBrokerInstanceIdsMap() {
    return getTableToBrokersMapFromExternalView(true, false);
  }

  private Map<String, List<String>> getTableToBrokersMapFromExternalView(boolean tableWithType, boolean useUrlFormat) {
    Map<String, Set<String>> brokerUrlsMap = new HashMap<>();
    try {
      byte[] brokerResourceNodeData = _zkClient.readData(BROKER_EXTERNAL_VIEW_PATH, true);
      brokerResourceNodeData = unpackZnodeIfNecessary(brokerResourceNodeData);
      JsonNode jsonObject = OBJECT_READER.readTree(getInputStream(brokerResourceNodeData));
      JsonNode brokerResourceNode = jsonObject.get("mapFields");

      Iterator<Map.Entry<String, JsonNode>> resourceEntries = brokerResourceNode.fields();
      while (resourceEntries.hasNext()) {
        Map.Entry<String, JsonNode> resourceEntry = resourceEntries.next();
        String resourceName = resourceEntry.getKey();
        String tableName =
            tableWithType ? resourceName : resourceName.replace(OFFLINE_SUFFIX, "").replace(REALTIME_SUFFIX, "");
        Set<String> brokerUrls = brokerUrlsMap.computeIfAbsent(tableName, k -> new HashSet<>());
        JsonNode resource = resourceEntry.getValue();
        Iterator<Map.Entry<String, JsonNode>> brokerEntries = resource.fields();
        while (brokerEntries.hasNext()) {
          Map.Entry<String, JsonNode> brokerEntry = brokerEntries.next();
          String brokerName = brokerEntry.getKey();
          if (brokerName.startsWith("Broker_") && "ONLINE".equals(brokerEntry.getValue().asText())) {
            if (useUrlFormat) {
              // Turn Broker_12.34.56.78_1234 into 12.34.56.78:1234
              String brokerHostPort = brokerName.replace("Broker_", "").replace("_", ":");
              brokerUrls.add(brokerHostPort);
            } else {
              brokerUrls.add(brokerName);
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception while reading External view from zookeeper", e);
      // ignore
    }
    Map<String, List<String>> tableToBrokersMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : brokerUrlsMap.entrySet()) {
      tableToBrokersMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return tableToBrokersMap;
  }

  private static byte[] unpackZnodeIfNecessary(byte[] znodeContents) {
    // Check for gzip header
    if (znodeContents[0] == 0x1F && znodeContents[1] == (byte) 0x8B) {
      try {
        GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(znodeContents));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int byteRead = inputStream.read();

        while (byteRead != -1) {
          outputStream.write(byteRead);
          byteRead = inputStream.read();
        }

        return outputStream.toByteArray();
      } catch (IOException e) {
        LOGGER.error("Failed to decompress znode contents", e);
        return znodeContents;
      }
    } else {
      // Doesn't look compressed, just return the contents verbatim
      return znodeContents;
    }
  }
}
