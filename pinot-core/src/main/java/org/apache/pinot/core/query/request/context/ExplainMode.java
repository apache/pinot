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

public enum ExplainMode {
  /// Explain is disabled, queries are executed normally.
  NONE,
  /// The mode used in single-stage.
  ///
  /// Each node is returned as a row with the following columns:
  ///
  /// 1. Operator: a String column with descriptions like `BROKER_REDUCE(limit:10)`
  /// 2. Operator_Id: an int id used to rebuild the parent relation
  /// 3. Parent_Id: an int id used to rebuild the parent relation
  DESCRIPTION,
  /// Information is returned in a [org.apache.pinot.core.plan.ExplainInfo] object.
  ///
  /// Each segment is returned as a row with the following columns:
  ///
  /// 1. plan: The plan in [org.apache.pinot.core.plan.ExplainInfo], encoded as JSON
  NODE
}
