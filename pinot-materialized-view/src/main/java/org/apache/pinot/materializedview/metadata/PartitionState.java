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
package org.apache.pinot.materializedview.metadata;


/// State of a materialized partition in the MV lifecycle.
///
///   - `VALID` – partition is up-to-date with base table data.
///   - `STALE` – base table data has changed since last materialization; partition
///     needs OVERWRITE.
///
/// Partition expiration is modeled by **absence** from the runtime metadata's
/// partition map, not as a separate state.  The DELETE task path removes the
/// map entry; the broker then treats that bucket as "not covered by the MV"
/// and routes those queries to the base table.
///
/// Encoded as a single character (`"V"` / `"S"`) for compact ZK storage.
public enum PartitionState {
  VALID("V"),
  STALE("S");

  private final String _code;

  PartitionState(String code) {
    _code = code;
  }

  public String encode() {
    return _code;
  }

  public static PartitionState decode(String code) {
    switch (code) {
      case "V":
        return VALID;
      case "S":
        return STALE;
      default:
        throw new IllegalArgumentException("Unknown PartitionState code: " + code);
    }
  }
}
