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
package com.linkedin.pinot.broker.pruner;

import java.util.HashMap;
import java.util.Map;


/**
 * Provider class for SegmentZKMetadataPruners.
 */
public class SegmentZKMetadataPrunerProvider {
  private SegmentZKMetadataPrunerProvider() {
  }

  private static final Map<String, Class<? extends SegmentZKMetadataPruner>> PRUNER_MAP = new HashMap<>();

  static {
    PRUNER_MAP.put("partitionzkmetadatapruner", PartitionZKMetadataPruner.class);
  }

  /**
   * Returns ZK metadata based segment pruner.
   *
   * @param prunerClassName Class name for the desired pruner.
   * @return Instance of pruner with the specified name.
   */
  public static SegmentZKMetadataPruner getSegmentPruner(String prunerClassName) {
    try {
      Class<? extends SegmentZKMetadataPruner> aClass = PRUNER_MAP.get(prunerClassName.toLowerCase());
      if (aClass != null) {
        return aClass.newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Exception caught while instantiating segment ZK metadata based pruner: " + prunerClassName, e);
    }
    throw new IllegalArgumentException("Unsupported segment ZK metadata based pruner: " + prunerClassName);
  }
}
