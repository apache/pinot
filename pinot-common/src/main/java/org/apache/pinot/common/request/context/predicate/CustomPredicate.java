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
package org.apache.pinot.common.request.context.predicate;

import org.apache.pinot.common.request.context.ExpressionContext;


/**
 * Base class for custom filter predicates registered via the plugin system.
 *
 * <p>Plugin authors should extend this class to define their own predicate types
 * (e.g., SemanticMatchPredicate). The {@link #getCustomTypeName()} method returns
 * the unique predicate name used to look up the corresponding filter operator factory
 * at query execution time.
 *
 * <p>Example:
 * <pre>
 * public class SemanticMatchPredicate extends CustomPredicate {
 *     private final String _queryText;
 *     private final int _topK;
 *
 *     public SemanticMatchPredicate(ExpressionContext lhs, String queryText, int topK) {
 *         super(lhs, "SEMANTIC_MATCH");
 *         _queryText = queryText;
 *         _topK = topK;
 *     }
 *     // ... getters, equals, hashCode, toString
 * }
 * </pre>
 */
public abstract class CustomPredicate extends BasePredicate {

  private final String _customTypeName;

  protected CustomPredicate(ExpressionContext lhs, String customTypeName) {
    super(lhs);
    _customTypeName = customTypeName;
  }

  @Override
  public final Type getType() {
    return Type.CUSTOM;
  }

  /**
   * Returns the unique name of this custom predicate type (e.g., "SEMANTIC_MATCH").
   * Used to look up the corresponding filter operator factory at execution time.
   */
  public String getCustomTypeName() {
    return _customTypeName;
  }
}
