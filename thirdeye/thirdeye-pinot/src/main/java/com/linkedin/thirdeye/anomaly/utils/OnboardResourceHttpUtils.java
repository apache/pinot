/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.cookie.BasicClientCookie;


/**
 * Utility class for calling OnboardResource endpoint to execute/schedule jobs
 */
public class OnboardResourceHttpUtils extends AbstractResourceHttpUtils {
  private static final String ONBOARD_RESOURCE_ENDPOINT = "/onboard/";
  private static final String FUNCTION_KEY = "function";
  private static final String CLONE_KEY = "clone";
  private static final String DELETE_EXISTING_ANOMALIES = "deleteExistingAnomalies";

  public OnboardResourceHttpUtils(String onboardHost, int onboardPost, String authToken) {
    super(new HttpHost(onboardHost, onboardPost));
    addAuthenticationCookie(authToken);
  }

  public String getClonedFunctionID(long functionId, String tag, boolean isCloneAnomaly) throws IOException{
    HttpPost req = new HttpPost(ONBOARD_RESOURCE_ENDPOINT + FUNCTION_KEY + "/" + functionId + "/" + CLONE_KEY + "/"
    + tag + "?cloneAnomaly=" + String.valueOf(isCloneAnomaly));
    return callJobEndpoint(req);
  }

  public String removeMergedAnomalies(long functionId, long startTime, long endTime) throws IOException{
    HttpPost req = new HttpPost(ONBOARD_RESOURCE_ENDPOINT + FUNCTION_KEY + "/" + functionId + "/" + DELETE_EXISTING_ANOMALIES
        + "?start="+startTime
        + "&end="+ endTime);
    return callJobEndpoint(req);
  }
}
