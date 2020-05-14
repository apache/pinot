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
package org.apache.pinot.server.upsert;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.metrics.AbstractMetrics;
import org.apache.pinot.core.segment.updater.WatermarkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpsertComponentContainerProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertComponentContainerProvider.class);

  public static final String UPSERT_COMPONENT_CONFIG_KEY = "watermarkManager.class";
  public static final String UPSERT_COMPONENT_CONFIG_DEFAULT = DefaultUpsertComponentContainer.class.getName();

  private final Configuration _conf;
  private UpsertComponentContainer _instance;

  public UpsertComponentContainerProvider(Configuration conf, AbstractMetrics metrics) {
    _conf = conf;
    String className = _conf.getString(UPSERT_COMPONENT_CONFIG_KEY, UPSERT_COMPONENT_CONFIG_DEFAULT);
    LOGGER.info("creating watermark manager with class {}", className);
    try {
      Class<UpsertComponentContainer> comonentContainerClass = (Class<UpsertComponentContainer>) Class.forName(className);
      Preconditions.checkState(comonentContainerClass.isAssignableFrom(WatermarkManager.class),
          "configured class not assignable from Callback class");
      _instance = comonentContainerClass.newInstance();
      _instance.init(_conf);
    } catch (Exception e) {
      LOGGER.error("failed to load watermark manager class", className, e);
      ExceptionUtils.rethrow(e);
    }
  }

  public UpsertComponentContainer getInstance() {
    return _instance;
  }
}
