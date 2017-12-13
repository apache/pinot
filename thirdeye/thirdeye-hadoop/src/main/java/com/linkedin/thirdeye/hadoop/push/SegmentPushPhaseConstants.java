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
package com.linkedin.thirdeye.hadoop.push;

/**
 * Class containing properties to be set for segment push
 */
public enum SegmentPushPhaseConstants {

  SEGMENT_PUSH_INPUT_PATH("segment.push.input.path"),
  SEGMENT_PUSH_UDF_CLASS("segment.push.udf.class"),
  SEGMENT_PUSH_CONTROLLER_HOSTS("segment.push.controller.hosts"),
  SEGMENT_PUSH_CONTROLLER_PORT("segment.push.controller.port"),
  SEGMENT_PUSH_START_TIME("segment.push.start.time"),
  SEGMENT_PUSH_END_TIME("segment.push.end.time");

  String name;

  SegmentPushPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}