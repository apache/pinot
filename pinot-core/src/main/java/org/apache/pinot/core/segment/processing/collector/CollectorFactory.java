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
package org.apache.pinot.core.segment.processing.collector;

import org.apache.pinot.spi.data.Schema;


/**
 * Factory for constructing a Collector from CollectorConfig
 */
public final class CollectorFactory {

  private CollectorFactory() {

  }

  public enum CollectorType {
    ROLLUP, CONCAT
    // TODO: add support for DEDUP
  }

  /**
   * Construct a Collector from the given CollectorConfig and schema
   */
  public static Collector getCollector(CollectorConfig collectorConfig, Schema pinotSchema) {
    Collector collector = null;
    switch (collectorConfig.getCollectorType()) {

      case ROLLUP:
        collector = new RollupCollector(collectorConfig, pinotSchema);
        break;
      case CONCAT:
        collector = new ConcatCollector();
        break;
    }
    return collector;
  }
}
