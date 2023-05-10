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
package org.apache.pinot.query.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PlanFragmentMetadata {
  public static final String PLAN_FRAGMENT_ID_KEY = "planFragmentId";
  private final Map<String, String> _customProperties = new HashMap<>();

  private List<String> _scannedTables = new ArrayList<>();

  public PlanFragmentMetadata() {
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  public void setScannedTables(List<String> scannedTables) {
    _scannedTables = scannedTables;
  }
}
