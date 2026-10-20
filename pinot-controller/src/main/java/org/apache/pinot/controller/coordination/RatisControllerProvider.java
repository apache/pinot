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
package org.apache.pinot.controller.coordination;

import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.transport.ControllerAccessPolicy;
import org.apache.pinot.coordination.ratis.transport.MetadataAudit;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy;
import org.apache.pinot.spi.env.PinotConfiguration;


/// Supplies explicitly qualified clock and certificate authorization inputs to the native controller lifecycle.
/// There is no default implementation: selecting a class or configuring numeric bounds does not qualify a clock.
/// Authorization policies must be current, nonblocking and thread-safe. This starter denies participant bindings
/// with serving capabilities; query listeners and controller HTTP resources are outside this lifecycle.
public interface RatisControllerProvider extends AutoCloseable {
  void init(PinotConfiguration configuration) throws Exception;

  MembershipClockSettings membershipClockSettings();

  ControllerAccessPolicy controllerAccessPolicy();

  ParticipantAccessPolicy participantAccessPolicy();

  /// Version-one policy for a previously unconfigured history. Existing committed policies are never replaced.
  Policy bootstrapPolicy();

  default MetadataAudit audit() {
    return (client, operation, outcome) -> { };
  }

  @Override
  default void close() throws Exception {
  }
}
