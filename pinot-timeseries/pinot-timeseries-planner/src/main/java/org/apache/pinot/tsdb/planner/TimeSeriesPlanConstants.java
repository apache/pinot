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
package org.apache.pinot.tsdb.planner;

public class TimeSeriesPlanConstants {
  private TimeSeriesPlanConstants() {
  }

  public static class WorkerRequestMetadataKeys {
    private static final String SEGMENT_MAP_ENTRY_PREFIX = "$segmentMapEntry#";

    private WorkerRequestMetadataKeys() {
    }

    public static final String LANGUAGE = "language";
    public static final String START_TIME_SECONDS = "startTimeSeconds";
    public static final String WINDOW_SECONDS = "windowSeconds";
    public static final String NUM_ELEMENTS = "numElements";
    public static final String DEADLINE_MS = "deadlineMs";

    public static boolean isKeySegmentList(String key) {
      return key.startsWith(SEGMENT_MAP_ENTRY_PREFIX);
    }

    public static String encodeSegmentListKey(String planId) {
      return SEGMENT_MAP_ENTRY_PREFIX + planId;
    }

    /**
     * Returns the plan-id corresponding to the encoded key.
     */
    public static String decodeSegmentListKey(String key) {
      return key.substring(SEGMENT_MAP_ENTRY_PREFIX.length());
    }
  }

  public static class WorkerResponseMetadataKeys {
    public static final String PLAN_ID = "planId";
    public static final String ERROR_TYPE = "errorType";
    public static final String ERROR_MESSAGE = "error";
  }
}
