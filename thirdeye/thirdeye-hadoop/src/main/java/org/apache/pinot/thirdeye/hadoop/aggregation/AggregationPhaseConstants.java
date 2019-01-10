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
package com.linkedin.thirdeye.hadoop.aggregation;
/**
 * This class contains the properties to be set for aggregation phase
 */
public enum AggregationPhaseConstants {

  AGG_PHASE_INPUT_PATH("aggregation.phase.input.path"),
  AGG_PHASE_AVRO_SCHEMA("aggregation.phase.avro.schema"),
  AGG_PHASE_OUTPUT_PATH("aggregation.phase.output.path"),
  AGG_PHASE_THIRDEYE_CONFIG("aggregation.phase.thirdeye.config");

  String name;

  AggregationPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
