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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AuthResource;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;

import static org.apache.pinot.thirdeye.common.constants.rca.MultiDimensionalSummaryConstants.*;
import static org.apache.pinot.thirdeye.common.constants.rca.RootCauseResourceConstants.*;


/**
 * ThirdEye API Client to communicate with ThirdEye RCA services
 */
public class ThirdEyeRcaRestClient extends AbstractRestClient {

  private final String DEFAULT_THIRDEYE_RCA_SERVICE_HOST = "localhost:1426";
  private final String THIRDEYE_RCA_CUBE_URI = "/dashboard/summary/autoDimensionOrder";
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

  /** For testing only, create a client with an alternate URLFactory. This constructor allows unit tests to mock server communication. */
  /* package private */  ThirdEyeRcaRestClient(HttpURLConnectionFactory connectionFactory, ThirdEyePrincipal principal) {
    super(connectionFactory);
    this.principal = principal;
    this.thirdEyeHost = DEFAULT_THIRDEYE_RCA_SERVICE_HOST;
  }

  public Map<String, Object> getAllHighlights(long anomalyId) throws IOException {
    TreeMap<String, String> queryParameters = new TreeMap<String, String>();
    queryParameters.put("anomalyId", String.valueOf(anomalyId));

    Map<String, String> headers = new HashMap<>();
    headers.put("Cookie", AuthResource.AUTH_TOKEN_NAME + "=" + principal.getSessionKey());
    return doGet(
        composeUrl(this.thirdEyeHost, THIRDEYE_RCA_HIGHLIGHTS_URI, queryParameters),
        this.thirdEyeHost,
        headers,
        new ResponseToMap());
  }

  public Map<String, Object> getCubeHighlights(long anomalyId) throws IOException {
    MergedAnomalyResultDTO anomalyDTO = DAORegistry.getInstance().getMergedAnomalyResultDAO().findById(anomalyId);

    long startTime = anomalyDTO.getStartTime();
    long endTime = anomalyDTO.getEndTime();
    TreeMap<String, String> queryParameters = new TreeMap<String, String>();
    queryParameters.put(METRIC_URN, anomalyDTO.getMetricUrn());
    queryParameters.put(BASELINE_START, String.valueOf(startTime - TimeUnit.DAYS.toMillis(7)));
    queryParameters.put(BASELINE_END, String.valueOf(endTime - TimeUnit.DAYS.toMillis(7)));
    queryParameters.put(CURRENT_START, String.valueOf(startTime));
    queryParameters.put(CURRENT_END, String.valueOf(endTime));

    queryParameters.put(CUBE_DEPTH, "3");
    queryParameters.put(CUBE_ONE_SIDE_ERROR, "false");
    queryParameters.put(CUBE_ORDER_TYPE, "auto");
    queryParameters.put(CUBE_SUMMARY_SIZE, "3");

    Map<String, String> headers = new HashMap<>();
    headers.put("Cookie", AuthResource.AUTH_TOKEN_NAME + "=" + principal.getSessionKey());
    return doGet(
        composeUrl(this.thirdEyeHost, THIRDEYE_RCA_CUBE_URI, queryParameters),
        this.thirdEyeHost,
        headers,
        new ResponseToMap());
  }

  public Map<String, Object> getRelatedEventHighlights(long anomalyId) throws IOException {
    // TODO: placeholder
    return new HashMap<>();
  }

  public Map<String, Object> getRelatedMetricHighlights(long anomalyId) throws IOException {
    // TODO: placeholder
    return new HashMap<>();
  }
}
