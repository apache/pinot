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
package org.apache.pinot.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import lombok.AllArgsConstructor;
import org.I0Itec.zkclient.ZkClient;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads brokers external view from Zookeeper
 */
@AllArgsConstructor
public class ExternalViewReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalViewReader.class);
  private static final ObjectReader OBJECT_READER = JsonUtils.DEFAULT_READER;
  public static final String BROKER_EXTERNAL_VIEW_PATH = "/EXTERNALVIEW/brokerResource";
  public static final String BROKER_INSTANCE_PATH = "/CONFIGS/PARTICIPANT";
  public static final String REALTIME_SUFFIX = "_REALTIME";
  public static final String OFFLINE_SUFFIX = "_OFFLINE";
  public static final String KEY_PINOT_TLS_PORT = "PINOT_TLS_PORT";
  public static final String KEY_SIMPLE_FIELDS = "simpleFields";
  public static final String KEY_HELIX_HOST = "HELIX_HOST";
  public static final String KEY_HELIX_PORT = "HELIX_PORT";

  private ZkClient _zkClient;

  @VisibleForTesting
  boolean _preferTlsPort;
  public ExternalViewReader(ZkClient zkClient) {
    this(zkClient, false);
  }

  public List<String> getLiveBrokers() {
    List<String> brokerUrls = new ArrayList<>();
    try {
      byte[] brokerResourceNodeData = _zkClient.readData(BROKER_EXTERNAL_VIEW_PATH, true);
      brokerResourceNodeData = unpackZnodeIfNecessary(brokerResourceNodeData);
      JsonNode jsonObject = OBJECT_READER.readTree(getInputStream(brokerResourceNodeData));
      JsonNode brokerResourceNode = jsonObject.get("mapFields");

      Iterator<Entry<String, JsonNode>> resourceEntries = brokerResourceNode.fields();
      while (resourceEntries.hasNext()) {
        JsonNode resource = resourceEntries.next().getValue();
        Iterator<Entry<String, JsonNode>> brokerEntries = resource.fields();
        while (brokerEntries.hasNext()) {
          Entry<String, JsonNode> brokerEntry = brokerEntries.next();
          String brokerName = brokerEntry.getKey();
          if (brokerName.startsWith("Broker_") && "ONLINE".equals(brokerEntry.getValue().asText())) {
            brokerUrls.add(getHostPort(brokerName));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception while reading External view from zookeeper", e);
      // ignore
    }
    return brokerUrls;
  }

  @VisibleForTesting
  String getHostPort(String brokerName) {
    // Turn Broker_12.34.56.78_1234 into 12.34.56.78:1234, try InstanceConfig first, naming convention as backup
    try {
      byte[] znStrBytes = _zkClient.readData(BROKER_INSTANCE_PATH + "/" + brokerName, true);
      if (znStrBytes != null) {
        JsonNode record = OBJECT_READER.readTree(new String(znStrBytes, StandardCharsets.UTF_8));
        if (record != null) {
          JsonNode simpleFields = record.get(KEY_SIMPLE_FIELDS);
          if (simpleFields != null) {
            JsonNode hostNameNode = simpleFields.get(KEY_HELIX_HOST);
            JsonNode tlsPortNode = simpleFields.get(KEY_PINOT_TLS_PORT);
            JsonNode helixPortNode = simpleFields.get(KEY_HELIX_PORT);
            String[] splitItems = brokerName.split("_");
            if (splitItems.length < 3) {
              throw new RuntimeException("Wrong BrokerName format " + brokerName);
            }
            String hostName = splitItems[1];
            if (hostNameNode != null && !Strings.isNullOrEmpty(hostNameNode.asText())) {
              hostName = hostNameNode.asText();
            }
            if (tlsPortNode != null && !Strings.isNullOrEmpty(tlsPortNode.asText()) && _preferTlsPort) {
              return hostName + ":" + tlsPortNode.asText();
            }
            if (helixPortNode != null && !Strings.isNullOrEmpty(helixPortNode.asText())) {
              return hostName + ":" + helixPortNode.asText();
            }
            return hostName + ":" + splitItems[splitItems.length - 1];
          }
        }
      }
    } catch (JsonProcessingException ex) {
      LOGGER.error("Failed to read broker instance config for {}. Return by naming convention", brokerName, ex);
    }
    return brokerName.replace("Broker_", "").replace("_", ":");
  }

  protected ByteArrayInputStream getInputStream(byte[] brokerResourceNodeData) {
    return new ByteArrayInputStream(brokerResourceNodeData);
  }

  public Map<String, List<String>> getTableToBrokersMap() {
    Map<String, Set<String>> brokerUrlsMap = new HashMap<>();
    try {
      byte[] brokerResourceNodeData = _zkClient.readData(BROKER_EXTERNAL_VIEW_PATH, true);
      brokerResourceNodeData = unpackZnodeIfNecessary(brokerResourceNodeData);
      JsonNode jsonObject = OBJECT_READER.readTree(getInputStream(brokerResourceNodeData));
      JsonNode brokerResourceNode = jsonObject.get("mapFields");

      Iterator<Entry<String, JsonNode>> resourceEntries = brokerResourceNode.fields();
      while (resourceEntries.hasNext()) {
        Entry<String, JsonNode> resourceEntry = resourceEntries.next();
        String resourceName = resourceEntry.getKey();
        String tableName = resourceName.replace(OFFLINE_SUFFIX, "").replace(REALTIME_SUFFIX, "");
        Set<String> brokerUrls = brokerUrlsMap.computeIfAbsent(tableName, k -> new HashSet<>());
        JsonNode resource = resourceEntry.getValue();
        Iterator<Entry<String, JsonNode>> brokerEntries = resource.fields();
        while (brokerEntries.hasNext()) {
          Entry<String, JsonNode> brokerEntry = brokerEntries.next();
          String brokerName = brokerEntry.getKey();
          if (brokerName.startsWith("Broker_") && "ONLINE".equals(brokerEntry.getValue().asText())) {
            brokerUrls.add(getHostPort(brokerName));
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Exception while reading External view from zookeeper", e);
      // ignore
    }
    Map<String, List<String>> tableToBrokersMap = new HashMap<>();
    for (Entry<String, Set<String>> entry : brokerUrlsMap.entrySet()) {
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
