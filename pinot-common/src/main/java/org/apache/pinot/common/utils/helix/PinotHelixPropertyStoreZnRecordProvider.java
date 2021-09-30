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

import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


public class PinotHelixPropertyStoreZnRecordProvider {

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _pathPrefix;

  private PinotHelixPropertyStoreZnRecordProvider() {
    _pathPrefix = null;
    _propertyStore = null;
  }

  private PinotHelixPropertyStoreZnRecordProvider(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String relativePathName) {
    _propertyStore = propertyStore;
    _pathPrefix = relativePathName;
  }

  public static PinotHelixPropertyStoreZnRecordProvider forSchema(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/SCHEMAS");
  }

  public static PinotHelixPropertyStoreZnRecordProvider forTable(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/CONFIGS/TABLES");
  }

  public static PinotHelixPropertyStoreZnRecordProvider forSegments(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    return new PinotHelixPropertyStoreZnRecordProvider(propertyStore, "/SEGMENTS");
  }

  public ZNRecord get(String name) {
    return _propertyStore.get(_pathPrefix + "/" + name, null, AccessOption.PERSISTENT);
  }

  public void set(String name, ZNRecord record) {
    _propertyStore.set(_pathPrefix + "/" + name, record, AccessOption.PERSISTENT);
  }

  public boolean exist(String path) {
    return _propertyStore.exists(_pathPrefix + "/" + path, AccessOption.PERSISTENT);
  }

  public String getRelativePath() {
    return _pathPrefix;
  }
}
