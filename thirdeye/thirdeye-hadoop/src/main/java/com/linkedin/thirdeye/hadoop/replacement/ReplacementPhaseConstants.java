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
package com.linkedin.thirdeye.hadoop.replacement;

/**
 * This class contains the properties to be set for replacement phase
 */
public enum ReplacementPhaseConstants {
  REPLACEMENT_PHASE_INPUT_PATH("replacement.phase.input.path"),
  REPLACEMENT_PHASE_TOPK_PATH("replacement.phase.topk.path"),
  REPLACEMENT_PHASE_INPUT_SCHEMA_PATH("replacement.phase.input.schema.path"),
  REPLACEMENT_PHASE_OUTPUT_PATH("replacement.phase.output.path"),
  REPLACEMENT_PHASE_OUTPUT_SCHEMA_PATH("replacement.phase.output.schema.path"),
  REPLACEMENT_PHASE_THIRDEYE_CONFIG("replacement.phase.thirdeye.config");

  String name;

  ReplacementPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
