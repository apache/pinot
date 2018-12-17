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
import java.util.Objects;


public class YamlTranslationResult {
  private static final String CRON_SCHEDULE_DEFAULT = "0 0 14 * * ? *";

  private final Map<String, Object> properties;
  private final Map<String, Object> components;
  private final String cron;

  public YamlTranslationResult() {
    this.properties = new HashMap<>();
    this.components = new HashMap<>();
    this.cron = CRON_SCHEDULE_DEFAULT;
  }

  public YamlTranslationResult(Map<String, Object> properties, Map<String, Object> components, String cron) {
    this.properties = properties;
    this.components = components;
    this.cron = cron;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Map<String, Object> getComponents() {
    return components;
  }

  public String getCron() {
    return cron;
  }

  YamlTranslationResult withProperties(Map<String, Object> properties) {
    return new YamlTranslationResult(properties, this.components, this.cron);
  }

  YamlTranslationResult withComponents(Map<String, Object> components) {
    return new YamlTranslationResult(this.properties, components, this.cron);
  }

  YamlTranslationResult withCron(String cron) {
    return new YamlTranslationResult(this.properties, components, cron);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof YamlTranslationResult)) {
      return false;
    }
    YamlTranslationResult that = (YamlTranslationResult) o;
    return Objects.equals(properties, that.properties) && Objects.equals(components, that.components) && Objects.equals(
        cron, that.cron);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, components, cron);
  }
}
