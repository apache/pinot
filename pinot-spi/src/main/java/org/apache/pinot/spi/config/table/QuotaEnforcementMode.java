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
package org.apache.pinot.spi.config.table;

/// Controls how a configured table quota (e.g. storage, query QPS) is enforced. Declared in increasing order of
/// strictness; code that has to pick one of several modes relies on the ordinal.
public enum QuotaEnforcementMode {
  /// Breaches of the quota are neither reported nor acted on.
  DISABLED,
  /// The quota is checked and breaches are logged/emitted as metrics, but the underlying action (rejecting a segment,
  /// pausing consumption, aborting a query) is not taken.
  SHADOW,
  /// The quota is checked and breaches block the underlying action. This is the default behavior.
  ENFORCED
}
