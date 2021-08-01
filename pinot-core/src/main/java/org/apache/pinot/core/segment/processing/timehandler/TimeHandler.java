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
package org.apache.pinot.core.segment.processing.timehandler;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Handler for time values within the records:
 * - Filter records based on the start/end time range
 * - Round time values
 * - Partition records based on time values
 */
public interface TimeHandler {
  enum Type {
    NO_OP, EPOCH
    // TODO: Support DATE_TIME handler which rounds and partitions time on calendar date boundary with timezone support
  }

  String DEFAULT_PARTITION = "0";

  /**
   * Filters/rounds the time value for the given row and returns the time partition, or {@code null} if the row is
   * filtered out. The time value is modified in-place.
   */
  @Nullable
  String handleTime(GenericRow row);
}
