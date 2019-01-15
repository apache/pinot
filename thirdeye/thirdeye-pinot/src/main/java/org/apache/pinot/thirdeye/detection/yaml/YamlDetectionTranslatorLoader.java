/*
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

package org.apache.pinot.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import java.lang.reflect.Constructor;
import java.util.Map;


/**
 * Loads the detection config translator fo a pipeline type
 */
public class YamlDetectionTranslatorLoader {
  private static final String PROP_PIPELINE_TYPE= "pipelineType";
  private static DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();

  public YamlDetectionConfigTranslator from(Map<String, Object> yamlConfig, DataProvider provider) throws Exception {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_PIPELINE_TYPE), "Pipeline type is missing.");
    String className = DETECTION_REGISTRY.lookupYamlConverter(yamlConfig.get(PROP_PIPELINE_TYPE).toString().toUpperCase());
    Constructor<?> constructor = Class.forName(className).getConstructor(Map.class, DataProvider.class);
    return (YamlDetectionConfigTranslator) constructor.newInstance(yamlConfig, provider);
  }

}
