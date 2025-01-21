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
package org.apache.pinot.controller.helix.core.util;

import java.util.Map;


public class FailureInjectionUtils {
  public static final String FAULT_BEFORE_COMMIT_END_METADATA = "FaultBeforeCommitEndMetadata";
  public static final String FAULT_BEFORE_IDEAL_STATE_UPDATE = "FaultBeforeIdealStateUpdate";
  public static final String FAULT_BEFORE_NEW_SEGMENT_METADATA_CREATION = "FaultBeforeNewSegmentCreation";

  private FailureInjectionUtils() {
  }

  public static void injectFailure(String faultTypeKey, Map<String, String> managerConfigs) {
    String faultTypeConfig = managerConfigs.getOrDefault(faultTypeKey, "false");
    if (Boolean.parseBoolean(faultTypeConfig)) {
      throw new RuntimeException("Injecting failure: " + faultTypeKey);
    }
  }
}
