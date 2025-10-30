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
package org.apache.pinot.controller.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.api.dto.PinotControllerJobMetadataDto;
import org.apache.pinot.controller.api.dto.PinotTableReloadStatusResponse;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.segment.spi.creator.name.SegmentNameUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;


@Singleton
public class PinotTableReloadStatusReporter {
  private static final Logger LOG = LoggerFactory.getLogger(PinotTableReloadStatusReporter.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;

  @Inject
  public PinotTableReloadStatusReporter(PinotHelixResourceManager pinotHelixResourceManager, Executor executor,
      HttpClientConnectionManager connectionManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _executor = executor;
    _connectionManager = connectionManager;
  }

  private static double computeEstimatedRemainingTimeInMinutes(PinotTableReloadStatusResponse finalResponse,
      double timeElapsedInMinutes) {
    int remainingSegments = finalResponse.getTotalSegmentCount() - finalResponse.getSuccessCount();

    double estimatedRemainingTimeInMinutes = -1;
    if (finalResponse.getSuccessCount() > 0) {
      estimatedRemainingTimeInMinutes =
          ((double) remainingSegments / (double) finalResponse.getSuccessCount()) * timeElapsedInMinutes;
    }
    return estimatedRemainingTimeInMinutes;
  }

  private static double computeTimeElapsedInMinutes(double submissionTime) {
    return ((double) System.currentTimeMillis() - submissionTime) / (1000.0 * 60.0);
  }

  private static int computeTotalSegments(Map<String, List<String>> serverToSegments) {
    int totalSegments = 0;
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      totalSegments += entry.getValue().size();
    }
    return totalSegments;
  }

  private static List<String> getServerUrls(BiMap<String, String> serverEndPoints,
      PinotControllerJobMetadataDto reloadJob,
      Map<String, List<String>> serverToSegments) {
    List<String> serverUrls = new ArrayList<>();
    for (Map.Entry<String, String> entry : serverEndPoints.entrySet()) {
      final String server = entry.getKey();
      final String endpoint = entry.getValue();
      serverUrls.add(constructReloadTaskStatusEndpoint(reloadJob, serverToSegments, endpoint, server));
    }
    return serverUrls;
  }

  private static String constructReloadTaskStatusEndpoint(PinotControllerJobMetadataDto reloadJob,
      Map<String, List<String>> serverToSegments, String endpoint, String server) {
    String reloadTaskStatusEndpoint = constructReloadStatusEndpoint(reloadJob, endpoint);
    if (reloadJob.getSegmentName() == null) {
      return reloadTaskStatusEndpoint;
    }

    List<String> segmentsForServer = serverToSegments.get(server);
    StringBuilder encodedSegmentsBuilder = new StringBuilder();
    if (!segmentsForServer.isEmpty()) {
      Iterator<String> segmentIterator = segmentsForServer.iterator();
      // Append first segment without a leading separator
      encodedSegmentsBuilder.append(URIUtils.encode(segmentIterator.next()));
      // Append remaining segments, each prefixed by the separator
      while (segmentIterator.hasNext()) {
        encodedSegmentsBuilder.append(SegmentNameUtils.SEGMENT_NAME_SEPARATOR)
            .append(URIUtils.encode(segmentIterator.next()));
      }
    }
    reloadTaskStatusEndpoint += "&segmentName=" + encodedSegmentsBuilder;
    return reloadTaskStatusEndpoint;
  }

  private static String constructReloadStatusEndpoint(PinotControllerJobMetadataDto reloadJob, String endpoint) {
    return endpoint + "/controllerJob/reloadStatus/" + reloadJob.getTableNameWithType() + "?reloadJobTimestamp="
        + reloadJob.getSubmissionTimeMs();
  }

  public PinotTableReloadStatusResponse getReloadJobStatus(String reloadJobId)
      throws InvalidConfigException {
    final PinotControllerJobMetadataDto reloadJobMetadata = getControllerJobMetadataFromZk(reloadJobId);
    final Map<String, List<String>> serverToSegments = getServerToSegments(reloadJobMetadata);

    final BiMap<String, String> serverEndPoints =
        _pinotHelixResourceManager.getDataInstanceAdminEndpoints(serverToSegments.keySet());
    final List<String> serverUrls = getServerUrls(serverEndPoints, reloadJobMetadata, serverToSegments);

    final CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(_executor, _connectionManager, serverEndPoints);
    final CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    final PinotTableReloadStatusResponse response = new PinotTableReloadStatusResponse().setSuccessCount(0)
        .setTotalSegmentCount(computeTotalSegments(serverToSegments))
        .setTotalServersQueried(serverUrls.size())
        .setTotalServerCallsFailed(serviceResponse._failedResponseCount);

    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      try {
        PinotTableReloadStatusResponse r =
            JsonUtils.stringToObject(responseString, PinotTableReloadStatusResponse.class);
        response.setSuccessCount(response.getSuccessCount() + r.getSuccessCount());
      } catch (Exception e) {
        response.setTotalServerCallsFailed(response.getTotalServerCallsFailed() + 1);
      }
    }

    // Add derived fields
    final double timeElapsedInMinutes = computeTimeElapsedInMinutes(reloadJobMetadata.getSubmissionTimeMs());
    final double estimatedRemainingTimeInMinutes =
        computeEstimatedRemainingTimeInMinutes(response, timeElapsedInMinutes);

    return response.setMetadata(reloadJobMetadata)
        .setTimeElapsedInMinutes(timeElapsedInMinutes)
        .setEstimatedTimeRemainingInMinutes(estimatedRemainingTimeInMinutes);
  }

  private PinotControllerJobMetadataDto getControllerJobMetadataFromZk(String reloadJobId) {
    Map<String, String> controllerJobZKMetadata =
        _pinotHelixResourceManager.getControllerJobZKMetadata(reloadJobId, ControllerJobTypes.RELOAD_SEGMENT);
    if (controllerJobZKMetadata == null) {
      throw new ControllerApplicationException(LOG, "Failed to find controller job id: " + reloadJobId,
          Response.Status.NOT_FOUND);
    }
    try {
      return JsonUtils.jsonNodeToObject(JsonUtils.objectToJsonNode(controllerJobZKMetadata),
          PinotControllerJobMetadataDto.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to convert metadata to PinotControllerJobDTO", e);
    }
  }

  @VisibleForTesting
  Map<String, List<String>> getServerToSegments(PinotControllerJobMetadataDto job) {
    return getServerToSegments(job.getTableNameWithType(), job.getSegmentName(), job.getInstanceName());
  }

  @VisibleForTesting
  Map<String, List<String>> getServerToSegments(String tableNameWithType, @Nullable String segmentNamesString,
      @Nullable String instanceName) {
    if (segmentNamesString == null) {
      // instanceName can be null or not null, and this method below can handle both cases.
      return _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType, instanceName, true);
    }
    // Skip servers and segments not involved in the segment reloading job.
    List<String> segmentNames = new ArrayList<>();
    Collections.addAll(segmentNames, StringUtils.split(segmentNamesString, SegmentNameUtils.SEGMENT_NAME_SEPARATOR));
    if (instanceName != null) {
      return Map.of(instanceName, segmentNames);
    }
    // If instance is null, then either one or all segments are being reloaded via current segment reload restful APIs.
    // And the if-check at the beginning of this method has handled the case of reloading all segments. So here we
    // expect only one segment name.
    checkState(segmentNames.size() == 1, "Only one segment is expected but got: %s", segmentNames);
    Map<String, List<String>> serverToSegments = new HashMap<>();
    Set<String> servers = _pinotHelixResourceManager.getServers(tableNameWithType, segmentNamesString);
    for (String server : servers) {
      serverToSegments.put(server, Collections.singletonList(segmentNamesString));
    }
    return serverToSegments;
  }
}
