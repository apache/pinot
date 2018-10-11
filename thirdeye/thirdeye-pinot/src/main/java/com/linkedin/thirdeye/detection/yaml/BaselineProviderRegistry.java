/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.detection.baseline.RuleBaselineProvider;
import java.util.Map;


public class BaselineProviderRegistry {

  private static Map<String, Object> baselineRegistry;

  private static final Map<String, Object> REGISTRY_MAP = ImmutableMap.<String, Object>builder()
      // rule detection
      .put("BASELINE", ImmutableMap.of("className", RuleBaselineProvider.class.getName()))
      .build();

  /**
   * Singleton
   */
  public static BaselineProviderRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Internal constructor.
   */
  private BaselineProviderRegistry() {
  }

  private static final BaselineProviderRegistry INSTANCE = new BaselineProviderRegistry();

}
