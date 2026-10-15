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
package org.apache.pinot.spi.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.utils.CommonConstants.ConfigChangeListenerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Singleton holding the cluster-wide default for the protobuf decoder's descriptor fallback: whether the last
/// successfully fetched (and resolved) descriptor is served when the remote descriptor fetch fails, so a transient
/// DNS / object-store outage does not permanently fail the CONSUMING transition. Enabled by default.
///
/// Dynamically updatable via the ZK cluster config
/// [ConfigChangeListenerConstants#PROTOBUF_DESCRIPTOR_FALLBACK_ENABLED] without a server restart; the value is
/// consulted each time a decoder is created. A table-level decoder prop ('descriptorFileFallbackEnabled')
/// overrides this cluster-wide value. Thread-safe.
public class ProtoBufDescriptorFallbackListener implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufDescriptorFallbackListener.class);
  private static final boolean DEFAULT_ENABLED = true;
  private static final ProtoBufDescriptorFallbackListener INSTANCE = new ProtoBufDescriptorFallbackListener();

  private final AtomicBoolean _enabled = new AtomicBoolean(DEFAULT_ENABLED);

  private ProtoBufDescriptorFallbackListener() {
  }

  public static ProtoBufDescriptorFallbackListener getInstance() {
    return INSTANCE;
  }

  public boolean isEnabled() {
    return _enabled.get();
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(ConfigChangeListenerConstants.PROTOBUF_DESCRIPTOR_FALLBACK_ENABLED)) {
      return;
    }
    boolean newEnabled =
        fromString(clusterConfigs.get(ConfigChangeListenerConstants.PROTOBUF_DESCRIPTOR_FALLBACK_ENABLED));
    boolean previousEnabled = _enabled.getAndSet(newEnabled);
    if (previousEnabled != newEnabled) {
      LOGGER.info("Updated cluster config: {} from {} to {}",
          ConfigChangeListenerConstants.PROTOBUF_DESCRIPTOR_FALLBACK_ENABLED, previousEnabled, newEnabled);
    }
  }

  private static boolean fromString(String value) {
    if (value == null || value.trim().isEmpty()) {
      return DEFAULT_ENABLED;
    }
    return Boolean.parseBoolean(value.trim());
  }

  @VisibleForTesting
  public void reset() {
    _enabled.set(DEFAULT_ENABLED);
  }

  @VisibleForTesting
  public void setEnabled(boolean enabled) {
    _enabled.set(enabled);
  }
}
