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
package org.apache.pinot.broker.routing;

import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.request.context.NativeQueryDeadline;


/// Broker-local query authority shared by planning, transport and delivery. Implementations bind opaque admissions
/// to one process registration and perform only local, bounded checks; these methods must never initiate metadata RPCs.
/// Callers retain the originating deadline and admission through final delivery instead of acquiring fresh authority.
public interface NativeBrokerQueryAdmission {
  NativeQueryDeadline originateDeadline(long requestedMillis);

  Admission admitQuery(String table, UUID queryId, UUID attemptId, NativeQueryDeadline deadline);

  void checkQueryDelivery(Admission admission);

  /// Runs a bounded, nonblocking callback atomically with respect to local authority invalidation.
  void deliverQuery(Admission admission, Runnable callback);

  /// Remaining shared deadline budget, after checking the original local permit and selected-server leases.
  long remainingMillis(Admission admission);

  /// Immutable captured planning inputs and exact destinations. Only the issuing authority can validate this token;
  /// constructing another implementation of this interface does not create serving authority.
  interface Admission {
    NativeBrokerRoutingImage image();

    List<NativeBrokerRoutingImage.ServerRequest> requests();

    NativeBrokerRoutingImage.PlanningMetadata planningMetadata(boolean ignoreCase);
  }
}
