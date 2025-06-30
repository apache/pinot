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
package org.apache.pinot.spi.config.table;

/**
 * Recovery mode which is used to decide how to recover a segment online in IS but having no completed (immutable)
 * replica on any server in pause-less ingestion
 */
public enum DisasterRecoveryMode {
  // ALWAYS means Pinot will always run the Disaster Recovery Job.
  ALWAYS,
  // DEFAULT means Pinot will skip the Disaster Recovery Job for tables like Dedup/Partial-Upsert where consistency
  // of data is higher in priority than availability. Features like Dedup/Partial-Upsert requires ingestion to only
  // happen in strict order (i.e. a segment ingested in past cannot be re-ingested if server has consumed the
  // following segments to it). So in case of above Disaster scenario, there's only a fix requiring bulk delete of
  // segments.
  DEFAULT
}
