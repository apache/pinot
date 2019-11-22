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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.lang.reflect.Field;
import org.apache.pinot.common.utils.EqualityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TenantConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantConfig.class);

  @JsonPropertyDescription("Broker tag prefix used by this table")
  private String _broker;

  @JsonPropertyDescription("Server tag prefix used by this table")
  private String _server;

  @JsonPropertyDescription("Overrides for tags")
  private TagOverrideConfig _tagOverrideConfig;

  public String getBroker() {
    return _broker;
  }

  public void setBroker(String broker) {
    _broker = broker;
  }

  public String getServer() {
    return _server;
  }

  public void setServer(String server) {
    _server = server;
  }

  public TagOverrideConfig getTagOverrideConfig() {
    return _tagOverrideConfig;
  }

  public void setTagOverrideConfig(TagOverrideConfig tagOverrideConfig) {
    _tagOverrideConfig = tagOverrideConfig;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    TenantConfig that = (TenantConfig) o;

    return EqualityUtils.isEqual(_broker, that._broker) && EqualityUtils.isEqual(_server, that._server);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_broker);
    result = EqualityUtils.hashCodeOf(result, _server);
    return result;
  }
}
