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
package org.apache.pinot.spi.config.table.ingestion;

/**
 * Policy to determine the behaviour of parallel consumption for Realtime Ingestion.
 */
public enum ParallelSegmentConsumptionPolicy {
  ALLOW_ALWAYS(true, true),
  ALLOW_DURING_BUILD_ONLY(true, false),
  ALLOW_DURING_DOWNLOAD_ONLY(false, true),
  DISALLOW_ALWAYS(false, false);

  /// Whether the consumption of next segment is allowed during build of the previous segment.
  private final boolean _allowedDuringBuild;
  /// Whether the consumption of next segment is allowed during download of the previous segment.
  private final boolean _allowedDuringDownload;

  ParallelSegmentConsumptionPolicy(boolean allowedDuringBuild, boolean allowedDuringDownload) {
    _allowedDuringBuild = allowedDuringBuild;
    _allowedDuringDownload = allowedDuringDownload;
  }

  public boolean isAllowedDuringBuild() {
    return _allowedDuringBuild;
  }

  public boolean isAllowedDuringDownload() {
    return _allowedDuringDownload;
  }
}
