/*
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

package org.apache.pinot.thirdeye.common.restclient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;

import static org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter.AUTH_TOKEN_NAME;
import static org.apache.pinot.thirdeye.common.constants.rca.MultiDimensionalSummaryConstants.*;
import static org.apache.pinot.thirdeye.common.constants.rca.RootCauseResourceConstants.*;


/**
 * ThirdEye API Client to communicate with ThirdEye RCA services
 */
public class ThirdEyeRcaRestClient extends AbstractRestClient {

  private final String DEFAULT_THIRDEYE_RCA_SERVICE_HOST = "localhost:1426";
  private final String THIRDEYE_RCA_CUBE_URI = "/dashboard/summary/autoDimensionOrder/v2";
  private final String THIRDEYE_RCA_HIGHLIGHTS_URI = "/rootcause/highlights";

  private ThirdEyePrincipal principal;
  private String thirdEyeHost;

  public ThirdEyeRcaRestClient(ThirdEyePrincipal principal, String host) {
    super();
    this.principal = principal;

    if (StringUtils.isEmpty(host)) {
      this.thirdEyeHost = DEFAULT_THIRDEYE_RCA_SERVICE_HOST;
    } else {
      this.thirdEyeHost = host;
    }
  }

  /** For testing only, create a client with an alternate Client. This constructor allows unit tests to mock server communication. */
  /* package private */  ThirdEyeRcaRestClient(Client client, ThirdEyePrincipal principal) {
    super(client);
    this.principal = principal;
    this.thirdEyeHost = DEFAULT_THIRDEYE_RCA_SERVICE_HOST;
  }

  /**
   * Fetch the likely root causes behind an anomaly
   */
  public Map<String, Object> getRootCauseHighlights(long anomalyId) throws IOException {
    TreeMap<String, String> queryParameters = new TreeMap<String, String>();
    queryParameters.put("anomalyId", String.valueOf(anomalyId));

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.put("Cookie", Arrays.asList(AUTH_TOKEN_NAME + "=" + principal.getSessionKey()));

    return doGet(
        composeUrl(this.thirdEyeHost, THIRDEYE_RCA_HIGHLIGHTS_URI, queryParameters),
        headers,
        new ResponseToMap());
  }

  /**
   * Retrieve the top dimension slices that explain the anomaly using the Cube Algorithm using wo1w
   */
  public Map<String, Object> getDimensionSummaryHighlights(String metricUrn, long startTime, long endTime,
      long cubeDepth, long cubeSummarySize, boolean isOneSidedError) throws IOException {
    TreeMap<String, String> queryParameters = new TreeMap<String, String>();
    queryParameters.put(METRIC_URN, metricUrn);
    queryParameters.put(BASELINE_START, String.valueOf(startTime - TimeUnit.DAYS.toMillis(7)));
    queryParameters.put(BASELINE_END, String.valueOf(endTime - TimeUnit.DAYS.toMillis(7)));
    queryParameters.put(CURRENT_START, String.valueOf(startTime));
    queryParameters.put(CURRENT_END, String.valueOf(endTime));

    queryParameters.put(CUBE_DEPTH, Long.toString(cubeDepth));
    queryParameters.put(CUBE_ONE_SIDE_ERROR, Boolean.toString(isOneSidedError));
    queryParameters.put(CUBE_SUMMARY_SIZE, Long.toString(cubeSummarySize));
    queryParameters.put(CUBE_ORDER_TYPE, "auto");

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.put("Cookie", Arrays.asList(AUTH_TOKEN_NAME + "=" + principal.getSessionKey()));

    return doGet(
        composeUrl(this.thirdEyeHost, THIRDEYE_RCA_CUBE_URI, queryParameters),
        headers,
        new ResponseToMap());
  }
}
