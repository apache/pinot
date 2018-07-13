/**
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
  private static final String MAX_QUERIES_PER_SECOND_FIELD_NAME = "maxQueriesPerSecond";

  @ConfigKey("storage")
  @ConfigDoc(value = "Storage allocated for this table", exampleValue = "10 GiB")
  private String _storage;
  private String _maxQueriesPerSecond;

  @Nullable
  public String getStorage() {
    return _storage;
  }

  public void setStorage(@Nullable String storage) {
    _storage = storage;
  }

  @Nullable
  public String getMaxQueriesPerSecond() {
    return _maxQueriesPerSecond;
  }

  public void setMaxQueriesPerSecond(@Nullable String maxQueriesPerSecond) {
    _maxQueriesPerSecond = maxQueriesPerSecond;
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
      quotaObject.put(MAX_QUERIES_PER_SECOND_FIELD_NAME, _maxQueriesPerSecond);
    } catch (JSONException e) {
      LOGGER.error("Failed to convert to json", e);
    }
    return quotaObject;
  }

  public String toString() {
    return toJson().toString();
  }

  public void validate() {
    if (!isStorageValid()) {
      LOGGER.error("Failed to convert storage quota config: {} to bytes", _storage);
      throw new ConfigurationRuntimeException("Failed to convert storage quota config: " + _storage + " to bytes");
    }
    if (!isMaxQueriesPerSecondValid()) {
      LOGGER.error("Failed to convert qps quota config: {}", _maxQueriesPerSecond);
      throw new ConfigurationRuntimeException("Failed to convert qps quota config: " + _maxQueriesPerSecond);
    }
  }

  public boolean isStorageValid() {
    return _storage == null || DataSize.toBytes(_storage) >= 0;
  }

  public boolean isMaxQueriesPerSecondValid() {
    Double qps = null;
    if (_maxQueriesPerSecond != null) {
      try {
        qps = Double.parseDouble(_maxQueriesPerSecond);
      } catch (NumberFormatException e) {
        LOGGER.error("Failed to convert qps quota config: {}", _maxQueriesPerSecond);
        return false;
      }
      if (qps <= 0) {
        LOGGER.error("Failed to convert qps quota config: {}", _maxQueriesPerSecond);
        return false;
      }
    }
    return _maxQueriesPerSecond == null || qps > 0;
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

    return EqualityUtils.isEqual(_storage, that._storage) && EqualityUtils.isEqual(
        _maxQueriesPerSecond, that._maxQueriesPerSecond);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_storage);
    result = EqualityUtils.hashCodeOf(result, _maxQueriesPerSecond);
    return result;
  }
}
