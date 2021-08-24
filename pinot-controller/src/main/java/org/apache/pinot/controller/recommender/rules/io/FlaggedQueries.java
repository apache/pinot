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
package org.apache.pinot.controller.recommender.rules.io;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Queries with warnings (expensive queries) and errors (invalid queries)
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
public class FlaggedQueries {
  Map<String, String> _queriesAndWarnings = new HashMap<>();

  public Map<String, String> getFlaggedQueries() {
    return _queriesAndWarnings;
  }

  @JsonIgnore
  public Set<String> getQueries() {
    return _queriesAndWarnings.keySet();
  }

  @JsonIgnore
  public void add(String query, String warning) {
    if (_queriesAndWarnings.containsKey(query)) {
      _queriesAndWarnings.put(query, _queriesAndWarnings.get(query) + " | " + warning);
    } else {
      _queriesAndWarnings.put(query, warning);
    }
  }
}
