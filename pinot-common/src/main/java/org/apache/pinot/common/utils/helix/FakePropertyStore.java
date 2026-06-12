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
package org.apache.pinot.common.utils.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.zookeeper.data.Stat;


public class FakePropertyStore extends ZkHelixPropertyStore<ZNRecord> {
  private Map<String, ZNRecord> _contents = new HashMap<>();
  private Map<String, Stat> _statMap = new HashMap<>();
  private IZkDataListener _listener = null;

  public FakePropertyStore() {
    super((ZkBaseDataAccessor<ZNRecord>) null, null, null);
  }

  @Override
  public ZNRecord get(String path, Stat stat, int options) {
    return _contents.get(path);
  }

  @Override
  public Stat getStat(String path, int options) {
    return _statMap.get(path);
  }

  @Override
  public List<String> getChildNames(String parentPath, int options) {
    return _contents.keySet().stream()
        .filter(e -> e.startsWith(parentPath))
        .map(e -> e.replaceFirst(parentPath + "/", "").split("/")[0])
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Bulk-read variant that returns the stored {@link ZNRecord} for each immediate child of
   * {@code parentPath}. Populates {@code stats} with the per-record stat in the same order as the
   * returned list. The real Helix implementation goes through {@code _baseAccessor}, which is
   * {@code null} in this fake; this override backs the same read shape from the in-memory map so
   * tests that rely on bulk reads work end-to-end.
   */
  @Override
  public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options) {
    List<String> childNames = getChildNames(parentPath, options);
    List<ZNRecord> out = new ArrayList<>(childNames.size());
    if (stats != null) {
      stats.clear();
    }
    for (String childName : childNames) {
      String childPath = parentPath + "/" + childName;
      ZNRecord record = _contents.get(childPath);
      if (record == null) {
        continue;
      }
      out.add(record);
      if (stats != null) {
        Stat stat = _statMap.get(childPath);
        stats.add(stat != null ? stat : new Stat());
      }
    }
    return out;
  }

  /**
   * Five-arg getChildren overload used by Pinot via {@code CommonConstants.Helix.ZkClient.RETRY_*}.
   * The retry parameters don't apply to the in-memory fake; delegates to the simpler signature
   * above.
   */
  @Override
  public List<ZNRecord> getChildren(String parentPath, List<Stat> stats, int options, int retryCount,
      int retryInterval) {
    return getChildren(parentPath, stats, options);
  }

  @Override
  public boolean exists(String path, int options) {
    return _contents.containsKey(path);
  }

  @Override
  public void subscribeDataChanges(String path, IZkDataListener listener) {
    _listener = listener;
  }

  @Override
  public boolean set(String path, ZNRecord record, int expectedVersion, int options) {
    try {
      setContentAndStat(path, record);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean set(String path, ZNRecord record, int options) {
    try {
      setContentAndStat(path, record);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean remove(String path, int options) {
    List<String> descendants = _contents.keySet().stream().filter(e -> e.startsWith(path)).collect(Collectors.toList());
    descendants.forEach(e -> _contents.remove(e));
    return true;
  }

  public void setContentAndStat(String path, ZNRecord record)
      throws Exception {
    _contents.put(path, record);
    Stat stat = new Stat();
    stat.setMtime(System.currentTimeMillis());
    _statMap.put(path, stat);
    if (_listener != null) {
      _listener.handleDataChange(path, record);
    }
  }

  @Override
  public void start() {
    // Don't try to connect to zk
  }
}
