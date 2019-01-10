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

package com.linkedin.thirdeye.hadoop.transform;

public enum TransformPhaseJobConstants {
  TRANSFORM_INPUT_SCHEMA("transform.input.schema"),
  TRANSFORM_INPUT_PATH("transform.input.path"),
  TRANSFORM_OUTPUT_PATH("transform.output.path"),
  TRANSFORM_OUTPUT_SCHEMA("transform.output.schema"),
  TRANSFORM_SOURCE_NAMES("transform.source.names"),
  TRANSFORM_UDF("transform.udf.class"),
  TRANSFORM_CONFIG_UDF("transform.config.udf.class"),
  TRANSFORM_NUM_REDUCERS("transform.num.reducers");

  String name;

  TransformPhaseJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
