/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.lang.reflect.Field;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TenantConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantConfig.class);

  private String broker;
  private String server;

  public String getBroker() {
    return broker;
  }

  public void setBroker(String broker) {
    this.broker = broker;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
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

    return EqualityUtils.isEqual(broker, that.broker) && EqualityUtils.isEqual(server, that.server);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(broker);
    result = EqualityUtils.hashCodeOf(result, server);
    return result;
  }
}
