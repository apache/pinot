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

package org.apache.pinot.thirdeye.tools;

import org.apache.pinot.thirdeye.anomaly.utils.AbstractResourceHttpUtils;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


public class DashboardHttpUtils extends AbstractResourceHttpUtils {
  private final String DEFAULT_PATH_TO_TIMESERIES = "/dashboard/data/timeseries?";
  private final String DATASET = "dataset";
  private final String METRIC = "metrics";
  private final String VIEW = "view";
  private final String DEFAULT_VIEW = "timeseries";
  private final String TIME_START = "currentStart";
  private final String TIME_END = "currentEnd";
  private final String GRANULARITY = "aggTimeGranularity";
  private final String DIMENSIONS = "dimensions"; // separate by comma
  private final String FILTERS = "filters";
  private final String EQUALS = "=";
  private final String AND = "&";

  public DashboardHttpUtils(String host, int port, String authToken) {
    super(new HttpHost(host, port));
    addAuthenticationCookie(authToken);
  }

  public String handleMetricViewRequest(String dataset, String metric, DateTime startTime, DateTime endTime,
      TimeUnit timeUnit, String dimensions, String filterJson, String timezone) throws Exception{
    DateTimeZone dateTimeZone = DateTimeZone.forID(timezone);
    startTime = new DateTime(startTime, dateTimeZone);
    endTime = new DateTime(endTime, dateTimeZone);
    // format http GET command
    StringBuilder urlBuilder = new StringBuilder(DEFAULT_PATH_TO_TIMESERIES);
    urlBuilder.append(DATASET + EQUALS + dataset + AND);
    urlBuilder.append(METRIC + EQUALS + metric + AND);
    urlBuilder.append(VIEW + EQUALS + DEFAULT_VIEW + AND);
    urlBuilder.append(TIME_START + EQUALS + Long.toString(startTime.getMillis()) + AND);
    urlBuilder.append(TIME_END + EQUALS + Long.toString(endTime.getMillis()) + AND);
    urlBuilder.append(GRANULARITY + EQUALS + timeUnit.name() + AND);
    if (dimensions != null && !dimensions.isEmpty()) {
      urlBuilder.append(DIMENSIONS + EQUALS + dimensions + AND);
    }
    if (filterJson != null && !filterJson.isEmpty()) {
      urlBuilder.append(FILTERS + EQUALS + URLEncoder.encode(filterJson, "UTF-8"));
    }

    HttpGet httpGet = new HttpGet(urlBuilder.toString());
    return callJobEndpoint(httpGet);
  }
}
