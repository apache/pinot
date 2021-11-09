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
package org.apache.pinot.common.tier;

import java.util.Map;
import javax.annotation.Nullable;


/**
 * Tier storage type which uses Pinot servers as storage
 */
public class PinotServerTierStorage implements TierStorage {
  private final String _serverTag;
  private final String _tierBackend;
  private final Map<String, String> _tierBackendProperties;

  public PinotServerTierStorage(String serverTag, @Nullable String tierBackend,
      @Nullable Map<String, String> tierBackendProperties) {
    _serverTag = serverTag;
    _tierBackend = tierBackend;
    _tierBackendProperties = tierBackendProperties;
  }

  /**
   * Returns the tag used to identify the servers being used as the tier storage
   */
  public String getServerTag() {
    return _serverTag;
  }

  @Nullable
  public String getTierBackend() {
    return _tierBackend;
  }

  @Nullable
  public Map<String, String> getTierBackendProperties() {
    return _tierBackendProperties;
  }

  @Override
  public String getType() {
    return TierFactory.PINOT_SERVER_STORAGE_TYPE;
  }

  @Override
  public String toString() {
    return "PinotServerTierStorage{_serverTag=" + _serverTag + ", _tierBackend=" + _tierBackend + "}";
  }
}
