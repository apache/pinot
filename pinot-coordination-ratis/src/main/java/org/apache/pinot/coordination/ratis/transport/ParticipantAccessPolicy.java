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
package org.apache.pinot.coordination.ratis.transport;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;


/// Current certificate-to-participant authority, separate from full controller metadata access. Implementations must
/// be thread-safe, nonblocking and return null to deny access. Every operation rechecks the current binding; a prior
/// token cannot supply its own identity, role or capabilities.
@FunctionalInterface
public interface ParticipantAccessPolicy {
  @Nullable
  ParticipantIdentity authorize(String certificateFingerprint);

  /// The role is included in every authoritative participant token. Capabilities are supplied to each renewal from
  /// this policy, so a narrowed policy cannot obtain a previous broader grant by choosing capabilities in its request.
  record ParticipantIdentity(String participantId, Role role, Set<Capability> capabilities) {
    public ParticipantIdentity {
      if (participantId == null || participantId.isBlank() || participantId.length() > 128 || role == null
          || capabilities == null
          || (role != Role.BROKER && capabilities.contains(Capability.BROKER_QUERY))
          || (role != Role.SERVER && capabilities.contains(Capability.SERVER_QUERY))
          || (role != Role.MINION && capabilities.contains(Capability.MINION_TASK))) {
        throw new IllegalArgumentException("Invalid supported participant identity, role or capabilities");
      }
      capabilities = Set.copyOf(capabilities);
    }
  }
}
