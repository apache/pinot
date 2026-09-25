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
package org.apache.pinot.core.query.distinct.table;

import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.function.LongConsumer;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.utils.ByteArray;


/// The `DistinctTable` stores the distinct records for the distinct queries.
public abstract class DistinctTable {
  // TODO: Tune the initial capacity
  public static final int MAX_INITIAL_CAPACITY = 10000;

  protected final DataSchema _dataSchema;
  protected final int _limit;
  protected final boolean _nullHandlingEnabled;

  // For single-column distinct null handling
  protected boolean _hasNull;
  protected int _limitWithoutNull;

  /// The hash only needs to be injective per column type; its consumers apply their own mixing on top.
  private static final Hasher64 VALUE_HASHER = Hashing.komihash5_0();

  /// Stand-in for the null marker, which is tracked as a flag rather than as a set entry.
  protected static final long NULL_VALUE_HASH = 0x9E3779B97F4A7C15L;

  /// Returned for a value whose stored type has no hash defined. Deliberately constant, so every such value
  /// collides: that undercounts, which is the safe direction. Do NOT replace it with something derived from the
  /// value (`toString()`, identity hash): a type whose representation differs between EQUAL values would make the
  /// count EXCEED the true distinct count, which is the one direction the consumers cannot tolerate.
  protected static final long UNKNOWN_TYPE_HASH = 0x5DEECE66DL;

  public DistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled) {
    _dataSchema = dataSchema;
    _limit = limit;
    _nullHandlingEnabled = nullHandlingEnabled;
    _limitWithoutNull = limit;
  }

  /// Returns the [DataSchema] of the DistinctTable.
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /// Returns the limit of the DistinctTable.
  public int getLimit() {
    return _limit;
  }

  /// Returns `true` if the DistinctTable has limit, `false` otherwise.
  public boolean hasLimit() {
    return _limit != Integer.MAX_VALUE;
  }

  /// Returns `true` if the DistinctTable has null handling enabled, `false` otherwise.
  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  /// Adds a null value into the DistinctTable.
  public void addNull() {
    assert _nullHandlingEnabled;
    _hasNull = true;
    _limitWithoutNull = _limit - 1;
  }

  /// Returns `true` if the DistinctTable has null, `false` otherwise.
  public boolean hasNull() {
    return _hasNull;
  }

  /// Returns `true` if the DistinctTable has order-by, `false` otherwise.
  public abstract boolean hasOrderBy();

  /// Feeds a 64-bit hash of every distinct value in this table into `sink`, including the null marker when present.
  ///
  /// Used by [org.apache.pinot.core.query.distinct.DistinctCardinalityTracker] to measure, across flush windows,
  /// how many distinct values a streaming distinct leaf has emitted. Hashes rather than values because the consumer
  /// only needs set semantics; a hash per value keeps the consumer free of any knowledge of stored types, and lets
  /// each subtype walk its own primitive set without boxing.
  ///
  /// Enumerates exactly what [#getRows()] and [#toDataTable()] emit, which is what makes the consumer's count a
  /// count of values actually streamed downstream.
  ///
  /// The one hard requirement is that equal values hash equally, so the count can never exceed the true number of
  /// distinct values. Collisions are therefore safe -- they undercount, which only delays the consumer's decision.
  ///
  /// @throws UnsupportedOperationException if this table holds segment-local dictionary ids rather than values, in
  ///     which case hashing them would measure nothing meaningful. The delegate runs before the null marker is
  ///     emitted, so a table that throws leaves `sink` untouched.
  public final void forEachValueHash(LongConsumer sink) {
    forEachNonNullValueHash(sink);
    if (_hasNull) {
      sink.accept(NULL_VALUE_HASH);
    }
  }

  /// Feeds a hash of every value in this table's own value set into `sink`. The null marker is handled by
  /// [#forEachValueHash], since it is a flag here rather than a set entry.
  protected abstract void forEachNonNullValueHash(LongConsumer sink);

  /// Merges another DistinctTable into the DistinctTable.
  public abstract void mergeDistinctTable(DistinctTable distinctTable);

  /// Merges a DataTable into the DistinctTable.
  public abstract boolean mergeDataTable(DataTable dataTable);

  /// Returns the number of unique rows within the DistinctTable.
  public abstract int size();

  /// Returns whether the DistinctTable is already satisfied.
  public abstract boolean isSatisfied();

  /// Returns the intermediate result as a list of rows (limit and sorting are not guaranteed).
  public abstract List<Object[]> getRows();

  /// Returns the intermediate result as a DataTable (limit and sorting are not guaranteed).
  public abstract DataTable toDataTable()
      throws IOException;

  /// Returns the final result as a ResultTable (limit applied, sorted if ordering is required).
  public abstract ResultTable toResultTable();

  protected static long hashValue(String value) {
    return VALUE_HASHER.hashCharsToLong(value);
  }

  protected static long hashValue(ByteArray value) {
    return VALUE_HASHER.hashBytesToLong(value.getBytes());
  }

  /// toString() distinguishes scale, matching BigDecimal.equals() and hence the set the value came out of.
  protected static long hashValue(BigDecimal value) {
    return VALUE_HASHER.hashCharsToLong(value.toString());
  }

  /// Folds one column's hash into a running multi-column row hash. Order-sensitive, which is what a tuple needs.
  protected static long mixValueHash(long hash, long valueHash) {
    return VALUE_HASHER.hashLongToLong(hash ^ valueHash);
  }
}
