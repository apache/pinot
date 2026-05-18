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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.spi.utils.JsonUtils;

/**
 * Client for rebalance-oriented administration operations.
 * Instances are lightweight views over the shared admin transport and are thread-safe as long as the parent
 * {@link PinotAdminClient} remains open.
 */
public class RebalanceAdminClient extends BaseServiceAdminClient {

  public RebalanceAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    super(transport, controllerAddress, headers);
  }

  /**
   * Gets table rebalance job status by job id.
   */
  public String getRebalanceStatus(String jobId)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/rebalanceStatus/" + jobId, null, _headers);
    return response.toString();
  }

  /**
   * Gets table rebalance job status by job id as a {@link ServerRebalanceJobStatusResponse}.
   */
  public ServerRebalanceJobStatusResponse getRebalanceStatusObject(String jobId)
      throws PinotAdminException {
    JsonNode response = _transport.executeGet(_controllerAddress, "/rebalanceStatus/" + jobId, null, _headers);
    try {
      return JsonUtils.jsonNodeToObject(response, ServerRebalanceJobStatusResponse.class);
    } catch (IOException e) {
      throw new PinotAdminException("Failed to deserialize rebalance status for job: " + jobId, e);
    }
  }

  /**
   * Triggers a table rebalance with arbitrary query parameters.
   */
  public String rebalanceTable(String tableName, Map<String, String> queryParams)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/rebalance", null,
        queryParams, _headers);
    return response.toString();
  }

  /**
   * Triggers a table rebalance with arbitrary query parameters and returns a {@link RebalanceResult}.
   */
  public RebalanceResult rebalanceTableObject(String tableName, Map<String, String> queryParams)
      throws PinotAdminException {
    JsonNode response = _transport.executePost(_controllerAddress, "/tables/" + tableName + "/rebalance", null,
        queryParams, _headers);
    try {
      return JsonUtils.jsonNodeToObject(response, RebalanceResult.class);
    } catch (IOException e) {
      throw new PinotAdminException("Failed to deserialize rebalance response for table: " + tableName, e);
    }
  }

  /**
   * Advanced rebalance API mirroring controller REST query params, returning a {@link RebalanceResult}.
   */
  public RebalanceResult rebalanceTable(String tableName, String tableType, boolean dryRun,
      boolean reassignInstances, boolean includeConsuming, boolean downtime, int minAvailableReplicas)
      throws PinotAdminException {
    return rebalanceTableObject(tableName, Map.of("type", tableType, "dryRun", String.valueOf(dryRun),
        "reassignInstances", String.valueOf(reassignInstances), "includeConsuming", String.valueOf(includeConsuming),
        "downtime", String.valueOf(downtime), "minAvailableReplicas", String.valueOf(minAvailableReplicas)));
  }

  /**
   * Cancels rebalance jobs for a table.
   */
  public List<String> cancelRebalance(String tableName, @Nullable String tableType)
      throws PinotAdminException {
    Map<String, String> queryParams = tableType != null ? Map.of("type", tableType) : null;
    JsonNode response =
        _transport.executeDelete(_controllerAddress, "/tables/" + tableName + "/rebalance", queryParams, _headers);
    // Controller returns a raw JSON array of job IDs, e.g. ["jobId1", "jobId2"]
    List<String> result = new ArrayList<>();
    if (response.isArray()) {
      for (JsonNode element : response) {
        result.add(element.asText());
      }
    }
    return result;
  }
}
