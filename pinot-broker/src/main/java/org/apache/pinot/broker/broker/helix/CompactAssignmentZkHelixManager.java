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
package org.apache.pinot.broker.broker.helix;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ChainedPathZkSerializer;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.pinot.common.utils.helix.CompactZNRecordSerializer;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// A spectator [ZKHelixManager] that reads table ideal states and external views with
/// [CompactZNRecordSerializer].
///
/// Helix creates the ZooKeeper client of the manager inside [#connect], with a default
/// [ChainedPathZkSerializer] that sends every path to [ZNRecordSerializer]. Right after that, this class replaces the
/// serializer of the client with one that sends `/<cluster>/IDEALSTATES/...` and `/<cluster>/EXTERNALVIEW/...` to
/// [CompactZNRecordSerializer]. The broker resource keeps [ZNRecordSerializer]: it is small and its readers are
/// outside of routing. The client keeps the serializer across session expiry, because Helix creates a new client only
/// in [#connect], and this class installs the serializer again after each [#connect].
///
/// The serializer applies to everything that reads through this manager: its data accessor, its `HelixAdmin`, its
/// property store (no path match) and the Helix callback handlers. All of them only read the records under those
/// paths. Writes still go through [ZNRecordSerializer].
///
/// Thread-safety: same as [ZKHelixManager]. The serializer is installed from the thread that calls [#connect],
/// before the caller registers any listener.
public class CompactAssignmentZkHelixManager extends ZKHelixManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactAssignmentZkHelixManager.class);

  public CompactAssignmentZkHelixManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress) {
    super(clusterName, instanceName, instanceType, zkAddress);
  }

  /// Creates the spectator Helix manager for the broker. Returns a [CompactAssignmentZkHelixManager] when
  /// [Broker#CONFIG_OF_ROUTING_COMPACT_ASSIGNMENT_READER_ENABLED] is on, otherwise the default Helix manager.
  public static HelixManager createSpectatorHelixManager(PinotConfiguration brokerConf, String clusterName,
      String instanceId, String zkServers) {
    if (brokerConf.getProperty(Broker.CONFIG_OF_ROUTING_COMPACT_ASSIGNMENT_READER_ENABLED,
        Broker.DEFAULT_ROUTING_COMPACT_ASSIGNMENT_READER_ENABLED)) {
      LOGGER.info("Using the compact ideal state and external view reader for cluster: {}", clusterName);
      return new CompactAssignmentZkHelixManager(clusterName, instanceId, InstanceType.SPECTATOR, zkServers);
    }
    return HelixManagerFactory.getZKHelixManager(clusterName, instanceId, InstanceType.SPECTATOR, zkServers);
  }

  @Override
  public void connect()
      throws Exception {
    super.connect();
    installCompactSerializer();
  }

  /// Replaces the serializer of the ZooKeeper client created by [#connect].
  @VisibleForTesting
  void installCompactSerializer() {
    RealmAwareZkClient zkClient = _zkclient;
    Preconditions.checkState(zkClient != null, "ZooKeeper client is not created for cluster: %s", getClusterName());
    zkClient.setZkSerializer(createPathBasedSerializer(getClusterName()));
  }

  /// Returns the serializer of the ZooKeeper client, for tests.
  @VisibleForTesting
  PathBasedZkSerializer getInstalledZkSerializer() {
    return _zkclient.getZkSerializer();
  }

  /// Returns a serializer that reads the table ideal states and external views of the given cluster with
  /// [CompactZNRecordSerializer], and everything else (including the broker resource) with [ZNRecordSerializer].
  @VisibleForTesting
  static PathBasedZkSerializer createPathBasedSerializer(String clusterName) {
    ZNRecordSerializer defaultSerializer = new ZNRecordSerializer();
    CompactZNRecordSerializer compactSerializer = new CompactZNRecordSerializer(defaultSerializer);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    String idealStatesPath = keyBuilder.idealStates().getPath();
    String externalViewsPath = keyBuilder.externalViews().getPath();
    // ChainedPathZkSerializer picks the longest matching path, so the broker resource paths override the parents
    return ChainedPathZkSerializer.builder(defaultSerializer)
        .serialize(idealStatesPath, compactSerializer)
        .serialize(externalViewsPath, compactSerializer)
        .serialize(keyBuilder.idealStates(Helix.BROKER_RESOURCE_INSTANCE).getPath(), defaultSerializer)
        .serialize(keyBuilder.externalView(Helix.BROKER_RESOURCE_INSTANCE).getPath(), defaultSerializer)
        .build();
  }
}
