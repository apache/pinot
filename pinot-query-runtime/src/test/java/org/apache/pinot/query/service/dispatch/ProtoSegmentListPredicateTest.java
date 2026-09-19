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
import org.apache.helix.model.ClusterConfig;
import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests for the static-config seed, the live cluster-config update path and the precedence between the two that
/// [ProtoSegmentListPredicate] documents.
public class ProtoSegmentListPredicateTest {
  private static final String KEY = CommonConstants.Broker.CONFIG_OF_MSE_PROTO_SEGMENT_LIST;

  @Test
  public void testCreateUsesShippedDefaultWhenUnset() {
    assertFalse(ProtoSegmentListPredicate.create(new PinotConfiguration()).isEnabled());
  }

  @Test
  public void testCreateReadsStaticBrokerConfig() {
    assertTrue(ProtoSegmentListPredicate.create(configWith("true")).isEnabled());
    assertFalse(ProtoSegmentListPredicate.create(configWith("false")).isEnabled());
  }

  /// An empty static value means "not set", exactly as an empty cluster-config value does, so the two paths cannot
  /// disagree about what a blank entry means.
  @Test
  public void testCreateTreatsEmptyStaticValueAsUnset() {
    assertFalse(ProtoSegmentListPredicate.create(configWith("")).isEnabled());
  }

  /// A typo must never enable the setting, on either path: the encoding is only safe on a fully upgraded cluster.
  @Test
  public void testUnrecognizedValueReadsAsDisabled() {
    assertFalse(ProtoSegmentListPredicate.create(configWith("ture")).isEnabled());

    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(true);
    predicate.onChange(Set.of(KEY), Map.of(KEY, "yes"));
    assertFalse(predicate.isEnabled());
  }

  @Test
  public void testOnChangeEnablesAndDisables() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(false);
    predicate.onChange(Set.of(KEY), Map.of(KEY, "true"));
    assertTrue(predicate.isEnabled());
    predicate.onChange(Set.of(KEY), Map.of(KEY, "false"));
    assertFalse(predicate.isEnabled());
  }

  @Test
  public void testOnChangeTrimsAndIsCaseInsensitive() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(false);
    predicate.onChange(Set.of(KEY), Map.of(KEY, "  TRUE  "));
    assertTrue(predicate.isEnabled());
  }

  @Test
  public void testOnChangeIgnoresChangeThatDoesNotTouchTheKey() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(true);
    // The key is present in the config snapshot but not in the changed set, so the value is kept.
    predicate.onChange(Set.of("some.other.key"), Map.of(KEY, "false", "some.other.key", "x"));
    assertTrue(predicate.isEnabled());
  }

  /// Clearing the key from cluster config falls back to the shipped default rather than to the static seed, which is
  /// the safe direction for a setting that is only valid on a fully upgraded cluster.
  @Test
  public void testOnChangeResetsToDefaultWhenValueRemovedOrEmpty() {
    ProtoSegmentListPredicate predicate = new ProtoSegmentListPredicate(true);
    predicate.onChange(Set.of(KEY), Map.of());
    assertFalse(predicate.isEnabled());

    predicate = new ProtoSegmentListPredicate(true);
    predicate.onChange(Set.of(KEY), Map.of(KEY, ""));
    assertFalse(predicate.isEnabled());
  }

  /// The documented precedence "cluster config beats the static seed" holds only because the change handler replays
  /// the current snapshot to a listener as it is registered. Pinned here against the real handler rather than left to
  /// the javadoc, since that replay is what lets an operator flip the encoding without restarting the brokers.
  @Test
  public void testRegistrationReplayLetsClusterConfigWinOverStaticSeed() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    handler.onClusterConfigChange(clusterConfig(Map.of(KEY, "true")), null);

    ProtoSegmentListPredicate predicate = ProtoSegmentListPredicate.create(configWith("false"));
    assertFalse(predicate.isEnabled(), "static seed applies before registration");

    assertTrue(handler.registerClusterConfigChangeListener(predicate));
    assertTrue(predicate.isEnabled(), "cluster config must win over the static seed");

    // Registration really did wire the listener up, so a later change still reaches it: this is the live disable
    // path an operator relies on to revert without a broker restart.
    handler.onClusterConfigChange(clusterConfig(Map.of(KEY, "false")), null);
    assertFalse(predicate.isEnabled());
  }

  /// The other half of the same contract: a replayed snapshot that does not carry the key must leave the static seed
  /// alone.
  @Test
  public void testRegistrationReplayPreservesStaticSeed() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    handler.onClusterConfigChange(clusterConfig(Map.of("some.other.key", "x")), null);

    ProtoSegmentListPredicate predicate = ProtoSegmentListPredicate.create(configWith("true"));
    assertTrue(handler.registerClusterConfigChangeListener(predicate));
    assertTrue(predicate.isEnabled(), "an unrelated cluster config must not clear the seed");
  }

  /// The ordering the broker actually uses: `BaseBrokerStarter` registers the predicate while it builds the
  /// multi-stage request handler, which is before the change handler is wired to Helix, so the replay at
  /// registration carries an empty snapshot. Cluster config must still win once the first real delivery arrives,
  /// which it does because an empty previous snapshot reports every key as changed.
  @Test
  public void testRegistrationBeforeFirstDeliveryStillLetsClusterConfigWin() {
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();

    ProtoSegmentListPredicate predicate = ProtoSegmentListPredicate.create(configWith("false"));
    assertTrue(handler.registerClusterConfigChangeListener(predicate));
    assertFalse(predicate.isEnabled(), "empty replay must leave the static seed alone");

    handler.onClusterConfigChange(clusterConfig(Map.of(KEY, "true")), null);
    assertTrue(predicate.isEnabled(), "first real delivery must apply the cluster config");

    handler.onClusterConfigChange(clusterConfig(Map.of(KEY, "false")), null);
    assertFalse(predicate.isEnabled());
  }

  private static ClusterConfig clusterConfig(Map<String, String> configs) {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    configs.forEach((key, value) -> clusterConfig.getRecord().setSimpleField(key, value));
    return clusterConfig;
  }

  private static PinotConfiguration configWith(String value) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(KEY, value);
    return config;
  }
}
