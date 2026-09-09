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
package org.apache.pinot.segment.local.upsert;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


/// Immutable partition context for an existing startup snapshot attempt. Contains no segment information or counts.
/// This context is not bound to individual bitmap files, which may be skipped, unchanged or subsequently overwritten.
///
/// The startup offset is an observation, not a verified boundary: predecessor reconciliation and background
/// mutations can overlap capture. Version 1 deliberately cannot certify replica divergence, even at equal offsets.
@JsonIgnoreProperties(ignoreUnknown = true)
public record UpsertSnapshotMetadata(int formatVersion, int partitionId, String consumingSegmentName,
                                    String startOffset, long capturedAtMillis) {
  public static final int FORMAT_VERSION = 1;

  public String getBoundaryStatus() {
    return "UNVERIFIED";
  }
}
