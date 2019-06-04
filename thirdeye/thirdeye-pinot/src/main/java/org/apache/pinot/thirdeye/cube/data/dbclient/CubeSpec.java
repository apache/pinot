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

import com.google.common.base.Preconditions;
import org.joda.time.DateTime;


/**
 * The spec that specifies the metric and its time range to be retrieved from the data base.
 */
public class CubeSpec {
  private CubeTag tag;
  private String metric;
  private DateTime startInclusive;
  private DateTime endExclusive;

  /**
   * Constructs a cube spec.
   *
   * @param tag the field name corresponds to the retrieved metric.
   * @param metric the name of the metric.
   * @param startInclusive start time of the metric, inclusive.
   * @param endExclusive the time of the metric, exclusive.
   */
  public CubeSpec(CubeTag tag, String metric, DateTime startInclusive, DateTime endExclusive) {
    setTag(tag);
    setMetric(metric);
    setStartInclusive(startInclusive);
    setEndExclusive(endExclusive);
  }

  /**
   * Returns the field name corresponds to the retrieved metric.
   *
   * @return the field name corresponds to the retrieved metric.
   */
  public CubeTag getTag() {
    return tag;
  }

  /**
   * Sets the field name corresponds to the retrieved metric.
   *
   * @param tag the field name corresponds to the retrieved metric.
   */
  public void setTag(CubeTag tag) {
    Preconditions.checkNotNull(tag);
    this.tag = tag;
  }

  /**
   * Returns the metric name.
   *
   * @return the metric name.
   */
  public String getMetric() {
    return metric;
  }

  /**
   * Sets the metric name.
   * @param metric the metric name.
   */
  public void setMetric(String metric) {
    Preconditions.checkNotNull(metric);
    this.metric = metric;
  }

  /**
   * Returns start time of the metric, inclusive.
   *
   * @return start time of the metric, inclusive.
   */
  public DateTime getStartInclusive() {
    return startInclusive;
  }

  /**
   * Sets start time of the metric, inclusive.
   * @param startInclusive start time of the metric, inclusive.
   */
  public void setStartInclusive(DateTime startInclusive) {
    Preconditions.checkNotNull(startInclusive);
    this.startInclusive = startInclusive;
  }

  /**
   * Returns end time of the metric, exclusive.
   *
   * @return end time of the metric, exclusive.
   */
  public DateTime getEndExclusive() {
    return endExclusive;
  }

  /**
   * Sets end time of the metric, exclusive.
   *
   * @param endExclusive end time of the metric, exclusive.
   */
  public void setEndExclusive(DateTime endExclusive) {
    Preconditions.checkNotNull(endExclusive);
    this.endExclusive = endExclusive;
  }
}
