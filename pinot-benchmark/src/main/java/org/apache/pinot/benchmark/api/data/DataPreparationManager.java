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
package org.apache.pinot.benchmark.api.data;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.benchmark.PinotBenchConf;
import org.apache.pinot.benchmark.api.resources.PinotBenchException;
import org.apache.pinot.benchmark.api.resources.SuccessResponse;
import org.apache.pinot.benchmark.common.utils.PinotBenchConstants.PinotServerType;
import org.apache.pinot.benchmark.common.utils.PinotClusterClient;
import org.apache.pinot.benchmark.common.utils.PinotClusterLocator;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.pql.parsers.utils.Pair;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataPreparationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPreparationManager.class);
  private static final String TMP_DIR_PREFIX = "tmp-";

  private final PinotBenchConf _conf;
  private final PinotClusterClient _pinotClusterClient;
  private final PinotClusterLocator _pinotClusterLocator;
  private final String _baseQueryDir;

  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;

  public DataPreparationManager(PinotBenchConf pinotBenchConf, PinotClusterClient pinotClusterClient,
      PinotClusterLocator pinotClusterLocator) {
    _conf = pinotBenchConf;
    _baseQueryDir = _conf.getPinotBenchBaseQueryDir();
    _pinotClusterClient = pinotClusterClient;
    _pinotClusterLocator = pinotClusterLocator;
  }

  public void start(HelixManager helixManager) {
    _helixManager = helixManager;
    _helixAdmin = _helixManager.getClusterManagmentTool();
  }

  public String listSegments(String clusterType, String tableName) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPinotClusterFromType(clusterType);
    try {
      SimpleHttpResponse response = _pinotClusterClient
          .sendGetRequest(PinotClusterClient.getListSegmentsHttpURI(pair.getFirst(), pair.getSecond(), tableName));
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER,
          "Exception when listing segments for Table: " + tableName + " from Cluster: " + clusterType,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String uploadSegment(FormDataMultiPart multiPart) {
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    InputStream segmentInputStream = bodyPart.getValueAs(InputStream.class);

    try {
      SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
          .getUploadSegmentRequest(PinotClusterClient.getUploadSegmentHttpURI(pair.getFirst(), pair.getSecond()),
              "segment", segmentInputStream));
      LOGGER.info("Added segment successfully: " + response.getResponse());
      return response.getResponse();
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public String convertAndUploadSegment(String tableName, FormDataMultiPart multiPart, String actionsStr) {
    if (actionsStr == null || actionsStr.isEmpty()) {
      return uploadSegment(multiPart);
    }
    String[] actions = actionsStr.split(",");
    int index = 0;

    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    InputStream segmentInputStream = bodyPart.getValueAs(InputStream.class);

    List<NameValuePair> nameValuePairs = new ArrayList<>();
    if ("convert".equalsIgnoreCase(actions[index])) {
      index++;
      nameValuePairs.add(new BasicNameValuePair("tableName", tableName));
    }

    if ("upload".equalsIgnoreCase(actions[index])) {
      try {
        SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
            .getUploadSegmentRequest(PinotClusterClient.getUploadSegmentHttpURI(pair.getFirst(), pair.getSecond()),
                "segment", segmentInputStream, null, nameValuePairs));
        return response.getResponse();
      } catch (HttpErrorStatusException e) {
        throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
      } catch (Exception e) {
        throw new PinotBenchException(LOGGER, "Exception when sending upload segment to perf cluster",
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    } else {
      return "No actions taken for Table: " + tableName;
    }
  }

  public SuccessResponse copyConvertAndUploadSegments(String originalTableName, String targetTableName,
      boolean diffOnly) {
    if (originalTableName == null || originalTableName.isEmpty()) {
      throw new PinotBenchException(LOGGER, "Original table name should not be null", Response.Status.BAD_REQUEST);
    }
    Pair<String, Integer> pair = _pinotClusterLocator.getPerfClusterPair();
    List<String> prodOfflineSegmentNames =
        getOfflineSegmentNames(PinotClusterLocator.PinotClusterType.PROD, originalTableName);

    if (diffOnly) {
      List<String> perfOfflineSegmentNames =
          getOfflineSegmentNames(PinotClusterLocator.PinotClusterType.PERF, targetTableName);
      removeExistingSegments(originalTableName, targetTableName, prodOfflineSegmentNames, perfOfflineSegmentNames);
    }

    String originalRawTableName = TableNameBuilder.extractRawTableName(originalTableName);
    int uploadedSegmentCount = 0;
    for (String offlineSegmentName : prodOfflineSegmentNames) {
      String url = getSegmentDownloadUrl(originalRawTableName, offlineSegmentName);
      if (url == null) {
        throw new PinotBenchException(LOGGER,
            "Could not find download url for Segment: " + offlineSegmentName + " of Table: " + originalTableName,
            Response.Status.INTERNAL_SERVER_ERROR);
      }

      try {
        SimpleHttpResponse response = _pinotClusterClient.sendRequest(PinotClusterClient
            .getUploadSegmentRequest(PinotClusterClient.getUploadSegmentHttpURI(pair.getFirst(), pair.getSecond()),
                targetTableName, "segment", url));

        uploadedSegmentCount++;
        LOGGER.info(response.getResponse());
      } catch (HttpErrorStatusException e) {
        throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
      } catch (Exception e) {
        throw new PinotBenchException(LOGGER, "Exception when uploading segment to perf cluster",
            Response.Status.INTERNAL_SERVER_ERROR, e);
      }
    }
    return new SuccessResponse(
        "Successfully uploaded " + uploadedSegmentCount + " segments from Table: " + originalTableName
            + " to perf Table: " + targetTableName);
  }

  private List<String> getOfflineSegmentNames(PinotClusterLocator.PinotClusterType pinotClusterType, String tableName) {
    String perfSegmentListStr = listSegments(pinotClusterType.name(), tableName);
    return parseOfflineSegmentNames(tableName, perfSegmentListStr);
  }

  private List<String> parseOfflineSegmentNames(String originalTableName, String segmentsList) {
    List<String> segmentNames = new ArrayList<>();
    try {
      JsonNode jsonArray = JsonUtils.stringToJsonNode(segmentsList);
      for (final JsonNode node : jsonArray) {
        JsonNode offlineJsonNode = node.get(PinotServerType.OFFLINE);
        if (offlineJsonNode != null) {
          if (offlineJsonNode.size() != 0) {
            for (JsonNode segmentNameJsonNode : offlineJsonNode) {
              segmentNames.add(segmentNameJsonNode.asText());
            }
          } else {
            throw new PinotBenchException(LOGGER, "There is no segment in Table: " + originalTableName,
                Response.Status.INTERNAL_SERVER_ERROR);
          }
        }
      }
      return segmentNames;
    } catch (IOException e) {
      throw new PinotBenchException(LOGGER, "IOException when parsing segment names for Table: " + originalTableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  private void removeExistingSegments(String originalTableName, String targetTableName,
      List<String> prodOfflineSegmentNames, List<String> perfOfflineSegmentNames) {
    for (String perfSegmentName : perfOfflineSegmentNames) {
      String prodSegment = perfSegmentName.replace(targetTableName, originalTableName);
      prodOfflineSegmentNames.remove(prodSegment);
    }
  }

  private String getSegmentDownloadUrl(String rawTableName, String segmentName) {
    Pair<String, Integer> pair = _pinotClusterLocator.getProdClusterPair();
    try {
      SimpleHttpResponse response = _pinotClusterClient.sendGetRequest(PinotClusterClient
          .getRetrieveSegmentMetadataHttpURI(pair.getFirst(), pair.getSecond(), rawTableName, segmentName));
      JsonNode jsonArray = JsonUtils.stringToJsonNode(response.getResponse());

      for (final JsonNode jsonNode : jsonArray) {
        for (JsonNode node : jsonNode) {
          JsonNode state = node.get("state");
          if (state != null) {
            return state.get("segment.offline.download.url").asText();
          }
        }
      }
      return null;
    } catch (HttpErrorStatusException e) {
      throw new PinotBenchException(LOGGER, e.getMessage(), e.getStatusCode(), e);
    } catch (Exception e) {
      throw new PinotBenchException(LOGGER,
          "Exception when getting metadata for Segment: " + segmentName + " of Table: " + rawTableName,
          Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }

  public SuccessResponse uploadQueries(String originalTableName, String targetTableName, FormDataMultiPart multiPart) {
    if (targetTableName == null || targetTableName.isEmpty()) {
      throw new PinotBenchException(LOGGER, "Target table name should not be null", Response.Status.BAD_REQUEST);
    }
    String targetRawTableName = TableNameBuilder.extractRawTableName(targetTableName);
    FormDataBodyPart bodyPart = multiPart.getFields().values().iterator().next().get(0);
    InputStream queriesInputStream = bodyPart.getValueAs(InputStream.class);

    File targetTableDir = new File(_baseQueryDir, targetTableName);
    targetTableDir.mkdirs();
    PrintWriter p;

    try (OutputStream outputStream = new FileOutputStream(new File(targetTableDir, "queries.txt"), false)) {
      if (originalTableName == null) {
        IOUtils.copy(queriesInputStream, outputStream);
      } else {
        BufferedReader reader = new BufferedReader(new InputStreamReader(queriesInputStream));
        p = new PrintWriter(outputStream);
        String originalRawTableName = TableNameBuilder.extractRawTableName(originalTableName);
        String line;
        while ((line = reader.readLine()) != null) {
          p.println(line.replace(originalRawTableName, targetRawTableName));
        }
        p.flush();
      }
      return new SuccessResponse("Successfully uploaded queries for Table: " + targetTableName);
    } catch (IOException e) {
      throw new PinotBenchException(LOGGER, "IOException when uploading queries. " + e.getMessage(),
          Response.Status.BAD_REQUEST, e);
    }
  }
}
