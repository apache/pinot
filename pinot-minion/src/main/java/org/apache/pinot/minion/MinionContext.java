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
package org.apache.pinot.minion;

import java.io.File;
import javax.net.ssl.SSLContext;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.core.minion.SegmentPurger;
import org.apache.pinot.spi.auth.AuthProvider;


/**
 * The <code>MinionContext</code> class is a singleton class which contains all minion related context.
 */
public class MinionContext {
  private static final MinionContext INSTANCE = new MinionContext();

  private MinionContext() {
  }

  public static MinionContext getInstance() {
    return INSTANCE;
  }

  private File _dataDir;
  private MinionMetrics _minionMetrics;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private HelixManager _helixManager;

  // For segment upload
  private SSLContext _sslContext;
  private AuthProvider _taskAuthProvider;

  // For PurgeTask
  private SegmentPurger.RecordPurgerFactory _recordPurgerFactory;
  private SegmentPurger.RecordModifierFactory _recordModifierFactory;

  public File getDataDir() {
    return _dataDir;
  }

  public void setDataDir(File dataDir) {
    _dataDir = dataDir;
  }

  @Deprecated
  public MinionMetrics getMinionMetrics() {
    return _minionMetrics;
  }

  @Deprecated
  public void setMinionMetrics(MinionMetrics minionMetrics) {
    _minionMetrics = minionMetrics;
  }

  public ZkHelixPropertyStore<ZNRecord> getHelixPropertyStore() {
    return _helixPropertyStore;
  }

  public void setHelixPropertyStore(ZkHelixPropertyStore<ZNRecord> helixPropertyStore) {
    _helixPropertyStore = helixPropertyStore;
  }

  public SSLContext getSSLContext() {
    return _sslContext;
  }

  public void setSSLContext(SSLContext sslContext) {
    _sslContext = sslContext;
  }

  public SegmentPurger.RecordPurgerFactory getRecordPurgerFactory() {
    return _recordPurgerFactory;
  }

  public void setRecordPurgerFactory(SegmentPurger.RecordPurgerFactory recordPurgerFactory) {
    _recordPurgerFactory = recordPurgerFactory;
  }

  public SegmentPurger.RecordModifierFactory getRecordModifierFactory() {
    return _recordModifierFactory;
  }

  public void setRecordModifierFactory(SegmentPurger.RecordModifierFactory recordModifierFactory) {
    _recordModifierFactory = recordModifierFactory;
  }

  public AuthProvider getTaskAuthProvider() {
    return _taskAuthProvider;
  }

  public void setTaskAuthProvider(AuthProvider taskAuthProvider) {
    _taskAuthProvider = taskAuthProvider;
  }

  public void setHelixManager(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  public HelixManager getHelixManager() {
    return _helixManager;
  }
}
