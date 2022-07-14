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
package org.apache.pinot.query.planner.hints;

import org.apache.calcite.rel.hint.RelHint;


/**
 * Provide certain relational hint to query planner for better optimization.
 */
public class PinotRelationalHints {
  public static final RelHint USE_HASH_DISTRIBUTE = RelHint.builder("USE_HASH_DISTRIBUTE").build();
  public static final RelHint USE_BROADCAST_DISTRIBUTE = RelHint.builder("USE_BROADCAST_DISTRIBUTE").build();
  public static final RelHint AGG_INTERMEDIATE_STAGE = RelHint.builder("AGG_INTERMEDIATE_STAGE").build();
  public static final RelHint AGG_LEAF_STAGE = RelHint.builder("AGG_LEAF_STAGE").build();

  private PinotRelationalHints() {
    // do not instantiate.
  }
}
