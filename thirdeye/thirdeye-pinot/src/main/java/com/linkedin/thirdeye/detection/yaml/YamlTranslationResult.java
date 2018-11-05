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

import java.util.HashMap;
import java.util.Map;


public class YamlTranslationResult {
  private Map<String, Object> properties;
  private Map<String, Object> components;

  public YamlTranslationResult() {
    this.properties = new HashMap<>();
    this.components = new HashMap<>();
  }

  public YamlTranslationResult(Map<String, Object> properties, Map<String, Object> components) {
    this.properties = properties;
    this.components = components;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Map<String, Object> getComponents() {
    return components;
  }

  YamlTranslationResult withProperties(Map<String, Object> properties) {
    return new YamlTranslationResult(properties, this.components);
  }

  YamlTranslationResult withComponents(Map<String, Object> components) {
    return new YamlTranslationResult(this.properties, components);
  }
}
