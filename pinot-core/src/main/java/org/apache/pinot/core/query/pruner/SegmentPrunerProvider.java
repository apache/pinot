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
package org.apache.pinot.core.query.pruner;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * A static SegmentPrunerProvider will give SegmentPruner instance based on prunerClassName and configuration.
 */
public class SegmentPrunerProvider {
  private SegmentPrunerProvider() {
  }

  private static final Map<String, Class<? extends SegmentPruner>> PRUNER_MAP = new HashMap<>();

  public static final String COLUMN_VALUE_SEGMENT_PRUNER_NAME = "columnvaluesegmentpruner";
  public static final String BLOOM_FILTER_SEGMENT_PRUNER_NAME = "bloomfiltersegmentpruner";
  public static final String SELECTION_QUERY_SEGMENT_PRUNER_NAME = "selectionquerysegmentpruner";

  static {
    PRUNER_MAP.put(COLUMN_VALUE_SEGMENT_PRUNER_NAME, ColumnValueSegmentPruner.class);
    PRUNER_MAP.put(BLOOM_FILTER_SEGMENT_PRUNER_NAME, BloomFilterSegmentPruner.class);
    PRUNER_MAP.put(SELECTION_QUERY_SEGMENT_PRUNER_NAME, SelectionQuerySegmentPruner.class);
  }

  @Nullable
  public static SegmentPruner getSegmentPruner(String prunerClassName, PinotConfiguration segmentPrunerConfig) {
    try {
      Class<? extends SegmentPruner> cls = PRUNER_MAP.get(prunerClassName.toLowerCase());
      if (cls != null) {
        SegmentPruner segmentPruner = cls.getDeclaredConstructor().newInstance();
        segmentPruner.init(segmentPrunerConfig);
        return segmentPruner;
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support SegmentPruner type with - " + prunerClassName, ex);
    }
    // being lenient to bad pruner names allows non-breaking evolution
    return null;
  }
}
