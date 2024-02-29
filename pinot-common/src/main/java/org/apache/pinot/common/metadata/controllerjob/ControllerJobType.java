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
package org.apache.pinot.common.metadata.controllerjob;

import com.google.common.collect.ImmutableSet;
import java.util.Set;


public class ControllerJobType {
  private ControllerJobType() {
  }
  public static final String RELOAD_SEGMENT = "RELOAD_SEGMENT";
  public static final String FORCE_COMMIT = "FORCE_COMMIT";
  public static final String TABLE_REBALANCE = "TABLE_REBALANCE";
  public static final String TENANT_REBALANCE = "TENANT_REBALANCE";
  public static final Set<String>
      VALID_CONTROLLER_JOB_TYPE = ImmutableSet.of(RELOAD_SEGMENT, FORCE_COMMIT, TABLE_REBALANCE, TENANT_REBALANCE);
}
