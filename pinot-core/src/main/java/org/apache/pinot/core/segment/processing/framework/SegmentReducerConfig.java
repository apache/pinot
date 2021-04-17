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
package org.apache.pinot.core.segment.processing.framework;

import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config for the reducer phase of SegmentProcessorFramework
 */
public class SegmentReducerConfig {

  private final Schema _pinotSchema;
  private final int _numRecordsPerPart;
  private final CollectorConfig _collectorConfig;

  public SegmentReducerConfig(Schema pinotSchema, CollectorConfig collectorConfig, int numRecordsPerPart) {
    _pinotSchema = pinotSchema;
    _numRecordsPerPart = numRecordsPerPart;
    _collectorConfig = collectorConfig;
  }

  /**
   * The Pinot schema
   */
  public Schema getPinotSchema() {
    return _pinotSchema;
  }

  /**
   * The number of records that a reducer should put in a single part file. This will directly control number of records per segment
   */
  public int getNumRecordsPerPart() {
    return _numRecordsPerPart;
  }

  /**
   * The CollectorConfig for the reducer
   */
  public CollectorConfig getCollectorConfig() {
    return _collectorConfig;
  }
}
