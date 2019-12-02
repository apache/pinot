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

package org.apache.pinot.thirdeye.detection.cache;

import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.CacheUtils;


/**
 * Class used to hold data needed to make a request to centralized cache.
 * Can be derived from ThirdEyeRequest, but it's meant to save some developer
 * effort and abstract away some of the code that needs to be written.
 */
public class ThirdEyeCacheRequest {

  ThirdEyeRequest request;
  private String metricUrn;
  private String dimensionKey;

  public ThirdEyeCacheRequest(ThirdEyeRequest request) {
    this.request = request;
    this.metricUrn = MetricEntity.fromMetric(request.getFilterSet().asMap(), this.getMetricId()).getUrn();
    this.dimensionKey = CacheUtils.hashMetricUrn(this.metricUrn);
  }

  /**
   * shorthand to create a ThirdEyeCacheRequest from a ThirdEyeRequest
   * @param request ThirdEyeRequest
   * @return ThirdEyeCacheRequest
   */
  public static ThirdEyeCacheRequest from(ThirdEyeRequest request) {
    return new ThirdEyeCacheRequest(request);
  }

  public ThirdEyeRequest getRequest() { return request; }

  public long getMetricId() { return request.getMetricFunctions().get(0).getMetricId(); }

  public String getMetricUrn() { return metricUrn; }

  public long getStartTimeInclusive() { return request.getStartTimeInclusive().getMillis(); }

  public long getEndTimeExclusive() { return request.getEndTimeExclusive().getMillis(); }

  public String getDimensionKey() { return dimensionKey; }
}
