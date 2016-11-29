/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.query.config;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration container for aggregation operators
 */
public class AggregationOperatorConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationOperatorConfig.class);

  private int hllLog2m = HllConstants.DEFAULT_LOG2M;
  private int totalRawDocs;
  /**
   * Instantiate from aggregation operator config from segment metadata
   * This is a static method to conveniently instantiate  AggregationOperatorConfig
   * from metadata.
   * @param meta the meta
   * @return the aggregation operator config
   */
  public static AggregationOperatorConfig instantiateFrom(SegmentMetadata meta) {
    AggregationOperatorConfig config = new AggregationOperatorConfig();
    config.hllLog2m = meta.getHllLog2m();
    config.totalRawDocs = meta.getTotalRawDocs();
    return config;
  }

  private AggregationOperatorConfig() {

  }

  public int getHllLog2m() {
    return hllLog2m;
  }

  public int getTotalRawDocs() {
    return totalRawDocs;
  }
}
