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

import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.common.utils.EqualityUtils;
import javax.annotation.Nullable;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class representing table quota configuration
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QuotaConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaConfig.class);
  private static final String STORAGE_FIELD_NAME = "storage";

  private String _storage;

  @Nullable
  public String getStorage() {
    return _storage;
  }

  public void setStorage(@Nullable String storage) {
    _storage = storage;
  }

  /**
   * Get the storage quota configured value in bytes
   * @return configured size in bytes or -1 if the value is missing or
   *    unparseable
   */
  public long storageSizeBytes() {
    return DataSize.toBytes(_storage);
  }

  public JSONObject toJson() {
    JSONObject quotaObject = new JSONObject();
    try {
      quotaObject.put(STORAGE_FIELD_NAME, _storage);
    } catch (JSONException e) {
      LOGGER.error("Failed to convert to json", e);
    }
    return quotaObject;
  }

  public String toString() {
    return toJson().toString();
  }

  public void validate() {
    if (_storage != null && DataSize.toBytes(_storage) < 0) {
      LOGGER.error("Failed to convert storage quota config: {} to bytes", _storage);
      throw new ConfigurationRuntimeException("Failed to convert storage quota config: " + _storage + " to bytes");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    QuotaConfig that = (QuotaConfig) o;

    return EqualityUtils.isEqual(_storage, that._storage);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_storage);
    return result;
  }
}
