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

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.instance.InstanceType;


/// A feature that a Pinot instance advertises in its instance config, with a revision.
///
/// Any component (server, broker, controller, minion) may advertise capabilities. Each capability names the area of
/// the feature in its own name (for example `MSE_` for the multi-stage engine) and declares the instance type that
/// advertises it. At startup an instance writes the map field `capabilities`
/// (`CommonConstants.Helix.Instance.CAPABILITIES_KEY`) of its instance config with
/// `HelixHelper.updateCapabilities`: the key is the name of a capability of its instance type and the value is its
/// [#getRevision()] as a decimal integer, for example `{"MSE_FEATURE": "1"}`. Another component reads the map,
/// for example with [InstanceCapabilityTracker], to decide whether it can use a feature that instances of an older
/// version do not support. A missing map, a missing key or a malformed value means revision 0: not supported.
///
/// The map also holds the reserved entry `pinotVersion`, the Pinot version that wrote it. A reader ignores the map
/// when that entry is not string-equal to the simple field `pinotVersion` of the same instance config, so the
/// capabilities of a version do not survive a rollback to a version that predates them. See
/// [InstanceCapabilityTracker].
///
/// The capabilities of this version are the constants of this class, listed in [#values()]. Contract:
/// - The revision of a capability only grows. Revision `n` implies support for everything that revisions below `n`
///   added. A change that old instances with the same capability cannot handle increments the revision.
/// - A revision is not a Pinot version. Nothing compares Pinot version strings.
/// - A new feature that old instances cannot handle adds a constant with revision 1, and adds it to [#values()] in
///   the same change as the code that supports the feature. An instance must never advertise a feature it cannot run.
/// - The names are persisted in ZooKeeper: never rename or remove a capability.
///
/// Two capabilities are equal when their names are equal. Instances are immutable and thread-safe.
public final class InstanceCapability {
  private static final List<InstanceCapability> VALUES = List.of();

  private final String _name;
  private final InstanceType _instanceType;
  private final int _revision;

  /// Package-private: production capabilities are the constants of this class. Tests create their own.
  InstanceCapability(String name, InstanceType instanceType, int revision) {
    // Upper case keeps the name apart from the reserved entry pinotVersion of the capabilities map.
    Preconditions.checkArgument(name.matches("[A-Z][A-Z0-9_]*"), "Capability name must be upper case: %s", name);
    Preconditions.checkArgument(revision > 0, "Revision of capability %s must be positive: %s", name, revision);
    _name = name;
    _instanceType = instanceType;
    _revision = revision;
  }

  /// Every capability of this version.
  public static List<InstanceCapability> values() {
    return VALUES;
  }

  /// The name of the capability, the key in the `capabilities` map field.
  public String name() {
    return _name;
  }

  /// The type of the instances that advertise this capability.
  public InstanceType getInstanceType() {
    return _instanceType;
  }

  /// The revision of this capability that this version of Pinot supports. Always positive.
  public int getRevision() {
    return _revision;
  }

  /// Every capability of [#values()] for the instance type with its revision, as a new mutable map. Empty when the
  /// instance type advertises no capability. `HelixHelper.updateCapabilities` adds the reserved `pinotVersion` entry
  /// and writes the result to the `capabilities` field.
  public static Map<String, String> toMapField(InstanceType instanceType) {
    return toMapField(VALUES, instanceType);
  }

  /// Same as [#toMapField(InstanceType)] over the given capabilities instead of [#values()].
  static Map<String, String> toMapField(Collection<InstanceCapability> capabilities, InstanceType instanceType) {
    Map<String, String> mapField = new HashMap<>();
    for (InstanceCapability capability : capabilities) {
      if (capability._instanceType == instanceType) {
        mapField.put(capability._name, Integer.toString(capability._revision));
      }
    }
    return mapField;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof InstanceCapability && _name.equals(((InstanceCapability) o)._name));
  }

  @Override
  public int hashCode() {
    return _name.hashCode();
  }

  @Override
  public String toString() {
    return _name;
  }
}
