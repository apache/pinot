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

package org.apache.pinot.spi.stream;

/**
 * Container that can be used for holding per-partition consumer lag calculated along standard dimensions such as
 * record offset, ingestion time etc.
 */
public class PartitionLagState {
  protected final static String NOT_CALCULATED = "NOT_CALCULATED";

  /**
   * Defines how far behind the current record's offset / pointer is from upstream latest record
   * The distance is based on actual record count.
   */
  public String getRecordsLag() {
    return NOT_CALCULATED;
  }

  /**
   * Defines how soon after record ingestion was the record consumed by Pinot. That is, the difference between the
   * time the record was consumed and the time at which the record was ingested upstream.
   *
   * @return Lag value in milliseconds
   */
  public String getAvailabilityLagMs() {
    return NOT_CALCULATED;
  }
}
