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
package org.apache.pinot.core.query.config;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;


/**
 * Config for SegmentPruner.
 */
public class SegmentPrunerConfig {
  private static final String SEGMENT_PRUNER_NAMES_KEY = "class";

  private final int _numSegmentPruners;
  private final List<String> _segmentPrunerNames;
  private final List<Configuration> _segmentPrunerConfigs;

  public SegmentPrunerConfig(Configuration segmentPrunerConfig) {
    String[] segmentPrunerNames = segmentPrunerConfig.getStringArray(SEGMENT_PRUNER_NAMES_KEY);
    _numSegmentPruners = segmentPrunerNames.length;
    _segmentPrunerNames = new ArrayList<>(_numSegmentPruners);
    _segmentPrunerConfigs = new ArrayList<>(_numSegmentPruners);
    for (String segmentPrunerName : segmentPrunerNames) {
      _segmentPrunerNames.add(segmentPrunerName);
      _segmentPrunerConfigs.add(segmentPrunerConfig.subset(segmentPrunerName));
    }
  }

  public int numSegmentPruners() {
    return _numSegmentPruners;
  }

  public String getSegmentPrunerName(int index) {
    return _segmentPrunerNames.get(index);
  }

  public Configuration getSegmentPrunerConfig(int index) {
    return _segmentPrunerConfigs.get(index);
  }
}
