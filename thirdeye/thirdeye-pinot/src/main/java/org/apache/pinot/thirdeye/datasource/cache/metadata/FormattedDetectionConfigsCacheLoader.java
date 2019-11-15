/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.datasource.cache.metadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.formatter.DetectionConfigFormatter;


/**
 * The cache loader to load the formatted detection configs
 */
public class FormattedDetectionConfigsCacheLoader implements MetadataCacheLoader<List<Map<String, Object>>> {
  private final DetectionConfigManager detectionConfigManager;
  private final DetectionConfigFormatter detectionConfigFormatter;

  public FormattedDetectionConfigsCacheLoader(DetectionConfigManager detectionConfigManager, MetricConfigManager metricConfigManager,
      DatasetConfigManager datasetConfigManager) {
    this.detectionConfigManager = detectionConfigManager;
    this.detectionConfigFormatter = new DetectionConfigFormatter(metricConfigManager, datasetConfigManager);
  }

  @Override
  public List<Map<String, Object>> loadMetaData() {
    return this.detectionConfigManager.findAll()
        .parallelStream()
        .map(this.detectionConfigFormatter::format)
        .collect(Collectors.toList());
  }
}
