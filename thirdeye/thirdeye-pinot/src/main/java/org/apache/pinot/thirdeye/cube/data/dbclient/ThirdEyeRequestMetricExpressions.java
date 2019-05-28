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

package org.apache.pinot.thirdeye.cube.data.dbclient;

import java.util.List;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;


/**
 * Classes to keep the information that can be used to construct the metric value from Pinot results.
 */
public class ThirdEyeRequestMetricExpressions {
  private ThirdEyeRequest thirdEyeRequest;
  private List<MetricExpression> metricExpressions;

  /**
   * Construct a pair of ThirdEye request and metric expression, which can be used to construct the metric value from
   * Pinot results.
   *
   * @param thirdEyeRequest the ThirdEye request.
   * @param metricExpressions the metric expression of the ThirdEye request.
   */
  public ThirdEyeRequestMetricExpressions(ThirdEyeRequest thirdEyeRequest, List<MetricExpression> metricExpressions) {
    this.thirdEyeRequest = thirdEyeRequest;
    this.metricExpressions = metricExpressions;
  }

  /**
   * Returns the ThirdEye request.
   *
   * @return the ThirdEye request.
   */
  public ThirdEyeRequest getThirdEyeRequest() {
    return thirdEyeRequest;
  }

  /**
   * Returns the metric expression of the ThirdEye request.
   *
   * @return the metric expression of the ThirdEye request.
   */
  public List<MetricExpression> getMetricExpressions() {
    return metricExpressions;
  }
}