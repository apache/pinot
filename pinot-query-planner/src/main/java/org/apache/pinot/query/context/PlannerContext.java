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
package org.apache.pinot.query.context;

import java.util.Map;


/**
 * PlannerContext is an object that holds all contextual information during planning phase.
 *
 * TODO: currently the planner context is not used since we don't support option or query rewrite. This construct is
 * here as a placeholder for the parsed out options.
 */
public class PlannerContext {
  private Map<String, String> _options;

  public void setOptions(Map<String, String> options) {
    _options = options;
  }

  public Map<String, String> getOptions() {
    return _options;
  }
}
