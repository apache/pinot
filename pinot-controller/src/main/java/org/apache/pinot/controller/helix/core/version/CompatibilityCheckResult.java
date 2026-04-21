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
package org.apache.pinot.controller.helix.core.version;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;


/**
 * Result of a cluster version compatibility check.
 *
 * <p>In v1 all checks are advisory — {@link #isOk()} being {@code false} produces warnings
 * but never blocks operations.
 */
public class CompatibilityCheckResult {

  /** Symbolic name of the check that was run (e.g. {@code ROLLOUT_ORDER}). */
  @JsonProperty("checkType")
  private final String _checkType;

  /** {@code true} when no violations were detected. */
  @JsonProperty("ok")
  private final boolean _ok;

  /** Human-readable summary. */
  @JsonProperty("message")
  private final String _message;

  /** Individual warning strings describing each detected violation. Empty when {@link #isOk()}. */
  @JsonProperty("warnings")
  private final List<String> _warnings;

  /** The cluster version snapshot used for this check. */
  @JsonProperty("clusterVersionSummary")
  private final ClusterVersionSummary _clusterVersionSummary;

  public CompatibilityCheckResult(String checkType, boolean ok, String message,
      List<String> warnings, ClusterVersionSummary clusterVersionSummary) {
    _checkType = checkType;
    _ok = ok;
    _message = message;
    _warnings = warnings == null
        ? Collections.emptyList() : Collections.unmodifiableList(warnings);
    _clusterVersionSummary = clusterVersionSummary;
  }

  public String getCheckType() {
    return _checkType;
  }

  public boolean isOk() {
    return _ok;
  }

  public String getMessage() {
    return _message;
  }

  public List<String> getWarnings() {
    return _warnings;
  }

  public ClusterVersionSummary getClusterVersionSummary() {
    return _clusterVersionSummary;
  }
}
