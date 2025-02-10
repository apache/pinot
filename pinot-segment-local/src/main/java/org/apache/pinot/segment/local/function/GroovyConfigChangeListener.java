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
package org.apache.pinot.segment.local.function;

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GroovyConfigChangeListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(GroovyConfigChangeListener.class);

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (changedConfigs.contains(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG)) {
      try {
        String configJson = clusterConfigs.get(CommonConstants.GROOVY_STATIC_ANALYZER_CONFIG);
        LOGGER.info("Updating Groovy Static Analyzer configuration with latest config: {}", configJson);
        GroovyStaticAnalyzerConfig config = GroovyStaticAnalyzerConfig.fromJson(configJson);
        GroovyFunctionEvaluator.setConfig(config);
      } catch (Exception e) {
        LOGGER.error("Failed to update Groovy Static Analyzer configuration", e);
      }
    } else {
      LOGGER.debug("Skip updating Groovy Static Analyzer configuration.");
    }
  }
}
