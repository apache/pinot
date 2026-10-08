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
package org.apache.pinot.core.data.table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;


/// Combine table for the BASE groups of a grouping-sets base-aggregation query. Like
/// [UnboundedConcurrentIndexedTable] with a result-size cap, but instead of silently dropping records whose
/// (new) base key arrives after the cap is hit, it RETAINS them in an overflow buffer. The combine later folds
/// the overflow records into the already-derived grouping-set groups (existing groups only), mirroring the
/// expansion path's behavior under the group limit: the grand total and coarse subtotals stay exact, and only
/// the overflowing fine-set groups are lost.
///
/// The overflow buffer is bounded (a multiple of the result size); if even the buffer overflows, further
/// unknown-key records are dropped and [#isOverflowTruncated()] reports it so the response can be flagged as
/// trimmed.
public class GroupingSetsBaseIndexedTable extends UnboundedConcurrentIndexedTable {
  /// Bounds the overflow buffer relative to the result size, so a pathological key space cannot exhaust heap.
  private static final int MAX_OVERFLOW_FACTOR = 10;

  private final int _maxOverflowRecords;
  private final ConcurrentLinkedQueue<Record> _overflowRecords = new ConcurrentLinkedQueue<>();
  private final AtomicInteger _numOverflowRecords = new AtomicInteger();
  private volatile boolean _full;
  private volatile boolean _overflowTruncated;

  public GroupingSetsBaseIndexedTable(DataSchema dataSchema, QueryContext queryContext, int resultSize,
      int initialCapacity, ExecutorService executorService) {
    super(dataSchema, false, queryContext, resultSize, initialCapacity, executorService);
    _maxOverflowRecords = (int) Math.min((long) resultSize * MAX_OVERFLOW_FACTOR, Integer.MAX_VALUE);
  }

  @Override
  protected void upsertWithoutOrderBy(Key key, Record record) {
    if (_full) {
      if (!updateExistingRecordIfPresent(key, record)) {
        // Unknown base key after the cap: retain the record so its contribution to the coarse grouping sets
        // (grand total, subtotals) is not lost. A record raced in by another thread right after the miss is
        // still correct: the later overflow fold merges into existing groups.
        if (_numOverflowRecords.incrementAndGet() <= _maxOverflowRecords) {
          _overflowRecords.add(record);
        } else {
          _overflowTruncated = true;
        }
      }
    } else {
      addOrUpdateRecord(key, record);
      if (_resultSize != Integer.MAX_VALUE && _lookupMap.size() >= _resultSize) {
        _full = true;
      }
    }
  }

  /// Base records whose key was not admitted (arrived after the cap); to be folded into existing derived
  /// groups. Call after all upserts are done.
  public List<Record> getOverflowRecords() {
    return new ArrayList<>(_overflowRecords);
  }

  /// Whether the base table hit its group cap (some base keys were not admitted as groups).
  public boolean isFull() {
    return _full;
  }

  /// Whether even the overflow buffer overflowed, i.e. some records were dropped entirely and the coarse
  /// totals are approximate.
  public boolean isOverflowTruncated() {
    return _overflowTruncated;
  }
}
