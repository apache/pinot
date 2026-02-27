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
package org.apache.pinot.segment.local.indexsegment.immutable;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMetadataPreProcessorRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataPreProcessorRegistry.class);
  private static SegmentMetadataPreProcessor _instance = null;

  private SegmentMetadataPreProcessorRegistry() {
  }

  /**
   * Get the registered instance of the pre-processor.
   *
   * @return The registered instance, or null if none registered
   */
  @Nullable
  public static SegmentMetadataPreProcessor getInstance() {
    return _instance;
  }

  /**
   * Register a pre-processor instance.
   *
   * @param instance The pre-processor instance to register
   */
  public static void setInstance(SegmentMetadataPreProcessor instance) {
    _instance = instance;
    LOGGER.info("Registered SegmentMetadataPreProcessor: {}", instance.getClass().getName());
  }
}
