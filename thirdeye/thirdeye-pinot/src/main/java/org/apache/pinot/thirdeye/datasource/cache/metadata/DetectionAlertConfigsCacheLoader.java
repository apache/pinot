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
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;


/**
 * The cache loader to load the list of detection alert config
 */
public class DetectionAlertConfigsCacheLoader implements MetadataCacheLoader<List<DetectionAlertConfigDTO>> {
  private final DetectionAlertConfigManager detectionAlertConfigManager;

  public DetectionAlertConfigsCacheLoader(DetectionAlertConfigManager detectionAlertConfigManager) {
    this.detectionAlertConfigManager = detectionAlertConfigManager;
  }

  @Override
  public List<DetectionAlertConfigDTO> loadMetaData() {
    return this.detectionAlertConfigManager.findAll();
  }
}
