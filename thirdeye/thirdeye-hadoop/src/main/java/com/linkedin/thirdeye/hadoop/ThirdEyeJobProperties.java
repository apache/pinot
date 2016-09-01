/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop;

public enum ThirdEyeJobProperties {
  THIRDEYE_FLOW_SCHEDULE("thirdeye.flow.schedule"), // HOURLY, DAILY
  THIRDEYE_PHASE("thirdeye.phase"), // segment_creation, segment_push
  THIRDEYE_ROOT("thirdeye.root"),
  THIRDEYE_COLLECTION("thirdeye.collection"),
  THIRDEYE_TIME_MIN("thirdeye.time.min"), // YYYY-mm-ddThh
  THIRDEYE_TIME_MAX("thirdeye.time.max"),
  INPUT_PATHS("input.paths"),
  THIRDEYE_MR_CONF("thirdeye.mr.conf"),
  THIRDEYE_PINOT_CONTROLLER_HOSTS("thirdeye.pinot.controller.hosts"),
  THIRDEYE_PINOT_CONTROLLER_PORT("thirdeye.pinot.controller.port"),
  THIRDEYE_BACKFILL_START_TIME("thirdeye.backfill.start.time"),
  THIRDEYE_BACKFILL_END_TIME("thirdeye.backfill.end.time"),
  THIRDEYE_NUM_REDUCERS("thirdeye.num.reducers");

  private final String propertyName;

  ThirdEyeJobProperties(String propertyName) {
    this.propertyName = propertyName;
  }

  public String getName() {
    return propertyName;
  }
}
