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

import org.apache.pinot.core.segment.processing.partitioner.PartitioningConfig;
import org.apache.pinot.core.segment.processing.transformer.RecordTransformerConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * Config for the mapper phase of SegmentProcessorFramework
 */
public class SegmentMapperConfig {

  private final Schema _pinotSchema;
  private final RecordTransformerConfig _recordTransformerConfig;
  private final PartitioningConfig _partitioningConfig;

  public SegmentMapperConfig(Schema pinotSchema, RecordTransformerConfig recordTransformerConfig,
      PartitioningConfig partitioningConfig) {
    _pinotSchema = pinotSchema;
    _recordTransformerConfig = recordTransformerConfig;
    _partitioningConfig = partitioningConfig;
  }

  /**
   * The Pinot schema
   */
  public Schema getPinotSchema() {
    return _pinotSchema;
  }

  /**
   * The RecordTransformerConfig for the mapper
   */
  public RecordTransformerConfig getRecordTransformerConfig() {
    return _recordTransformerConfig;
  }

  /**
   * The PartitioningConfig for the mapper
   */
  public PartitioningConfig getPartitioningConfig() {
    return _partitioningConfig;
  }
}
