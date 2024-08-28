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
package org.apache.pinot.tsdb.spi;

import java.time.Duration;


/**
 * A time-series request received by the Pinot Broker. This is passed to the {@link TimeSeriesLogicalPlanner} so
 * each query language can parse and plan the query based on their spec.
 * <br/>
 * <br/>
 * <b>Notes:</b>
 * <ul>
 *   <li>[start, end] are both inclusive.</li>
 *   <li>
 *     The result can contain time values outside [start, end], though we generally recommend to keep your results
 *     within the requested range. This decision is left to the time-series query language implementations. In some
 *     cases, returning data outside the requested time-range can help (e.g. for debugging purposes when you are
 *     computing moving 1d sum but are only looking at data for the last 12 hours).
 *   </li>
 *   <li>stepSeconds is used to define the default resolution for the query</li>
 *   <li>
 *     Some query languages allow users to change the resolution via a function, and in those cases the returned
 *     time-series may have a resolution different than stepSeconds
 *   </li>
 *   <li>
 *    The query execution may scan and process data outside of the time-range [start, end]. The actual data scanned
 *    and processed is defined by the {@link TimeBuckets} used by the operator.
 *   </li>
 * </ul>
 */
public class RangeTimeSeriesRequest {
  /** Engine allows a Pinot cluster to support multiple Time-Series Query Languages. */
  private final String _engine;
  /** Query is the raw query sent by the caller. */
  private final String _query;
  /** Start time of the time-window being queried. */
  private final long _startSeconds;
  /** End time of the time-window being queried. */
  private final long _endSeconds;
  /**
   * <b>Optional</b> field which the caller can use to suggest the default resolution for the query. Language
   * implementations can choose to skip this suggestion and choose their own resolution based on their semantics.
   */
  private final long _stepSeconds;
  /** E2E timeout for the query. */
  private final Duration _timeout;

  public RangeTimeSeriesRequest(String engine, String query, long startSeconds, long endSeconds, long stepSeconds,
      Duration timeout) {
    _engine = engine;
    _query = query;
    _startSeconds = startSeconds;
    _endSeconds = endSeconds;
    _stepSeconds = stepSeconds;
    _timeout = timeout;
  }

  public String getEngine() {
    return _engine;
  }

  public String getQuery() {
    return _query;
  }

  public long getStartSeconds() {
    return _startSeconds;
  }

  public long getEndSeconds() {
    return _endSeconds;
  }

  public long getStepSeconds() {
    return _stepSeconds;
  }

  public Duration getTimeout() {
    return _timeout;
  }
}
