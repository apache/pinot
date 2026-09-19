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
package org.apache.pinot.query.service.dispatch;

import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The cluster-level default for encoding leaf-stage segment lists as native protobuf fields of the worker metadata,
/// read by [QueryDispatcher] on every request that does not carry an explicit
/// [CommonConstants.Broker.Request.QueryOptionKey#PROTO_SEGMENT_LIST] override.
///
/// The value is seeded from the static broker configuration and can then be changed through cluster config, on the
/// same key, without restarting the brokers. That matters because the setting is only safe once every server of the
/// cluster understands the proto fields: an older server finds no segments under them, concludes the worker is not a
/// leaf-stage worker and fails its leaf stage. Operators therefore want to turn it on at the exact moment a rolling
/// upgrade completes, and to turn it back off immediately if it misbehaves, neither of which should cost a broker
/// restart. Cluster config wins over the static seed because [org.apache.pinot.common.config
/// .DefaultClusterConfigChangeHandler] replays the current cluster config to a listener as soon as it is registered;
/// clearing the key from cluster config falls back to [CommonConstants.Broker#DEFAULT_MSE_PROTO_SEGMENT_LIST], not to
/// the static seed.
///
/// Thread-safety: `_enabled` is `volatile`, so [#isEnabled()] stays lock-free on the request path. [#onChange] is
/// `synchronized` only so that the `previous -> new` pair in its log line cannot interleave with another delivery. It
/// does *not* order deliveries: the change handler invokes listeners outside its own lock, so a delivery computed
/// from an older snapshot can still be applied after a newer one and leave a stale value until the next
/// cluster-config change.
@ThreadSafe
public class ProtoSegmentListPredicate implements PinotClusterConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoSegmentListPredicate.class);
  private static final String KEY = CommonConstants.Broker.CONFIG_OF_MSE_PROTO_SEGMENT_LIST;
  private static final String ENABLE_WARNING =
      "Every server of the cluster must already run a version that understands the proto segment list fields. "
          + "Leaf stages routed to an older server will fail. Set it back to false to revert.";

  private volatile boolean _enabled;

  public ProtoSegmentListPredicate(boolean enabled) {
    _enabled = enabled;
  }

  /// Seeds the value from the static broker configuration. NOTE: the Helix manager is not necessarily connected when
  /// this is called, so a cluster-config override is applied later through [#onChange].
  public static ProtoSegmentListPredicate create(PinotConfiguration brokerConf) {
    String rawValue = brokerConf.getProperty(KEY);
    boolean enabled = rawValue == null || rawValue.isEmpty()
        ? CommonConstants.Broker.DEFAULT_MSE_PROTO_SEGMENT_LIST : parseBoolean(rawValue);
    LOGGER.info("Initialized {} with value: {}", KEY, enabled);
    if (enabled) {
      LOGGER.warn("{} is enabled in the static broker config. {}", KEY, ENABLE_WARNING);
    }
    return new ProtoSegmentListPredicate(enabled);
  }

  public boolean isEnabled() {
    return _enabled;
  }

  @Override
  public synchronized void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!changedConfigs.contains(KEY)) {
      return;
    }
    String value = clusterConfigs.get(KEY);
    boolean previous = _enabled;
    _enabled = value == null || value.isEmpty()
        ? CommonConstants.Broker.DEFAULT_MSE_PROTO_SEGMENT_LIST : parseBoolean(value);
    if (previous == _enabled) {
      return;
    }
    LOGGER.info("Updated {} from: {} to: {}", KEY, previous, _enabled);
    if (_enabled) {
      LOGGER.warn("{} was enabled live via cluster config. {}", KEY, ENABLE_WARNING);
    }
  }

  /// [Boolean#parseBoolean(String)] semantics (anything but `true` reads as `false`), plus a warning so that a typo
  /// does not silently disable the setting. Reading the static seed as a raw string rather than through
  /// [PinotConfiguration#getProperty(String, boolean)] is deliberate: that conversion is equally lenient but silent.
  private static boolean parseBoolean(String value) {
    String trimmed = value.trim();
    if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
      LOGGER.warn("Unrecognized boolean value '{}' for {}, reading it as false", value, KEY);
    }
    return Boolean.parseBoolean(trimmed);
  }
}
