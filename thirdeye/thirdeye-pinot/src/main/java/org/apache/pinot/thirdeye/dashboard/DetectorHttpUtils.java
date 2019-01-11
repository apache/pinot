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

package org.apache.pinot.thirdeye.dashboard;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;

import org.apache.pinot.thirdeye.anomaly.utils.AbstractResourceHttpUtils;
import org.apache.http.impl.cookie.BasicClientCookie;


/**
 * Utility classes for calling detector endpoints to execute/schedule jobs
 */
public class DetectorHttpUtils extends AbstractResourceHttpUtils {

  private static String ANOMALY_JOBS_ENDPOINT = "/api/anomaly-jobs/";
  private static String EMAIL_JOBS_ENDPOINT = "/api/email-jobs/";
  private static String ADHOC = "/ad-hoc";

  public DetectorHttpUtils(String detectorHost, int detectorPort, String authToken) {
    super(new HttpHost(detectorHost, detectorPort));
    addAuthenticationCookie(authToken);
  }

  public String enableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ANOMALY_JOBS_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String disableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(ANOMALY_JOBS_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String runAdhocAnomalyFunction(String id, String start, String end)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ANOMALY_JOBS_ENDPOINT + id + ADHOC + "?start=" + start + "&end=" + end);
    return callJobEndpoint(req);
  }

  public String enableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(EMAIL_JOBS_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String disableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(EMAIL_JOBS_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String runAdhocEmailConfiguration(String id)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(EMAIL_JOBS_ENDPOINT + id + ADHOC);
    return callJobEndpoint(req);
  }
}
