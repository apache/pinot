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
package org.apache.pinot.broker.requesthandler;

import org.apache.helix.HelixManager;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.pinot.common.utils.helix.ClusterVersionTracker;
import org.apache.pinot.common.version.PinotVersion;


/// Enables the window k-way merge wire node only for homogeneous live broker/server release versions.
/// SNAPSHOT builds remain excluded because equal snapshot versions can have different protocol implementations.
/// The shared tracker updates on Helix callbacks; query threads read only its volatile result.
@BatchMode(enabled = false)
@PreFetch(enabled = false)
public class KWayMergeSupportPredicate extends ClusterVersionTracker {
  public KWayMergeSupportPredicate(HelixManager manager) {
    this(manager, PinotVersion.VERSION);
  }

  KWayMergeSupportPredicate(HelixManager manager, String version) {
    super(manager, version, false, true);
  }
}
