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
package com.linkedin.thirdeye.hadoop.backfill;

/**
 * This class contains the properties to be set for backfill phase
 */
public enum BackfillPhaseConstants {

  BACKFILL_PHASE_CONTROLLER_HOST("backfill.phase.controller.host"),
  BACKFILL_PHASE_CONTROLLER_PORT("backfill.phase.controller.port"),
  BACKFILL_PHASE_START_TIME("backfill.phase.start.time"),
  BACKFILL_PHASE_END_TIME("backfill.phase.end.time"),
  BACKFILL_PHASE_TABLE_NAME("backfill.phase.table.name"),
  BACKFILL_PHASE_OUTPUT_PATH("backfill.phase.output.path");

  String name;

  BackfillPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
