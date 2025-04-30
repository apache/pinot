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
package org.apache.pinot.core.query.request.context;

import java.util.List;


public class TableSegmentsContext {
  private final String _tableName;
  private final List<String> _segments;
  private final List<String> _optionalSegments;

  public TableSegmentsContext(String tableName, List<String> segments, List<String> optionalSegments) {
    _tableName = tableName;
    _segments = segments;
    _optionalSegments = optionalSegments;
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getSegments() {
    return _segments;
  }

  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }
}
