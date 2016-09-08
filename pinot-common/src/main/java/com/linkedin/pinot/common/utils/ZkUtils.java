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
package com.linkedin.pinot.common.utils;

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * TODO Document me!
 *
 */
public class ZkUtils {
  /**
   * Returns a handle to the propertyStore
   *
   * @param helixManager * A valid helix manager bound to a cluster.
   * @param clusterName The pathname under propertyStore that we want a handle for
   * @return A handle to the propertyStore.
   *
   * @note: Setting up a watch can lead to connection problems with zookeeper during a re-connect from the client. The
   * client ends up sending all the path names in the session request, and that can exceed the max size of a session
   * request if there are too many nodes, causing the connection to be dropped and retried (with the same parameters).
   */
  public static ZkHelixPropertyStore<ZNRecord> getZkPropertyStore(HelixManager helixManager, String clusterName) {
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        (ZkBaseDataAccessor<ZNRecord>) helixManager.getHelixDataAccessor().getBaseDataAccessor();
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);

    ZkHelixPropertyStore<ZNRecord> propertyStore = new ZkHelixPropertyStore<ZNRecord>(baseAccessor, propertyStorePath, null);

    return propertyStore;
  }
}
