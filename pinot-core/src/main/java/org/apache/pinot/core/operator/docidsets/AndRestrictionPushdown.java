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
package org.apache.pinot.core.operator.docidsets;

import com.google.common.base.Preconditions;


/// How an [AndDocIdSet] pushes the document ids that it has matched so far into its children. Resolved once per query,
/// see `QueryContext#getAndRestrictionPushdown()`. Immutable.
///
/// - [Strategy#OFF]: no push-down. The scan children of an AND with an index child are still evaluated on the merged
///   index document ids, eagerly, as before the push-down existed.
/// - [Strategy#EAGER]: the composite (AND/OR/NOT) children are evaluated on the matched document ids too, and every
///   step returns a bitmap. Best for a query that reads every matching document, such as an aggregation.
/// - [Strategy#STREAMING]: the scans and the composite children are evaluated lazily on the matched document ids, one
///   chunk at a time, so a query that stops early only pays for the chunks it reads. An AND falls back to
///   [Strategy#EAGER] when [#getMaxDocsToRead()] is at least the number of its candidate documents, because it then
///   reads them all anyway.
public final class AndRestrictionPushdown {
  public enum Strategy {
    OFF, EAGER, STREAMING
  }

  public static final AndRestrictionPushdown OFF = new AndRestrictionPushdown(Strategy.OFF, Integer.MAX_VALUE);
  public static final AndRestrictionPushdown EAGER = new AndRestrictionPushdown(Strategy.EAGER, Integer.MAX_VALUE);

  private final Strategy _strategy;
  private final int _maxDocsToRead;

  private AndRestrictionPushdown(Strategy strategy, int maxDocsToRead) {
    _strategy = strategy;
    _maxDocsToRead = maxDocsToRead;
  }

  /// @param maxDocsToRead the most matching documents that the query reads from a segment: its limit plus its offset
  public static AndRestrictionPushdown streaming(int maxDocsToRead) {
    Preconditions.checkArgument(maxDocsToRead >= 0, "Invalid maxDocsToRead: %s", maxDocsToRead);
    return new AndRestrictionPushdown(Strategy.STREAMING, maxDocsToRead);
  }

  /// Maps the former boolean flag: `true` is [#EAGER], `false` is [#OFF].
  public static AndRestrictionPushdown of(boolean enabled) {
    return enabled ? EAGER : OFF;
  }

  public Strategy getStrategy() {
    return _strategy;
  }

  /// Returns whether the composite children may be reached through the candidate document ids, eagerly or lazily.
  public boolean isEnabled() {
    return _strategy != Strategy.OFF;
  }

  /// Returns the most matching documents that the query reads from a segment. Only meaningful for
  /// [Strategy#STREAMING].
  public int getMaxDocsToRead() {
    return _maxDocsToRead;
  }

  @Override
  public String toString() {
    return _strategy == Strategy.STREAMING ? "STREAMING(" + _maxDocsToRead + ")" : _strategy.name();
  }
}
