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

import java.io.IOException;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkSnapshot;


/// Coherent linearizable projection of one authenticated minion's task ledger and exact input dependencies.
/// Implementations validate its current live session, include no other minion's task inputs and fail closed when
/// the complete projection exceeds the supplied bounds. Pre-ready hydration does not authorize a task claim.
public interface TaskAccess {
  WorkSnapshot taskSnapshot(ParticipantToken minion, int maxRecords, int maxBytes, long timeoutMillis)
      throws IOException;
}
