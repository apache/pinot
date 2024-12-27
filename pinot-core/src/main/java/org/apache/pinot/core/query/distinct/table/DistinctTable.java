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

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;


/**
 * The {@code DistinctTable} stores the distinct records for the distinct queries.
 */
public abstract class DistinctTable {
  // TODO: Tune the initial capacity
  public static final int MAX_INITIAL_CAPACITY = 10000;

  protected final DataSchema _dataSchema;
  protected final int _limit;
  protected final boolean _nullHandlingEnabled;

  // For single-column distinct null handling
  protected boolean _hasNull;
  protected int _limitWithoutNull;

  public DistinctTable(DataSchema dataSchema, int limit, boolean nullHandlingEnabled) {
    _dataSchema = dataSchema;
    _limit = limit;
    _nullHandlingEnabled = nullHandlingEnabled;
    _limitWithoutNull = limit;
  }

  /**
   * Returns the {@link DataSchema} of the DistinctTable.
   */
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  /**
   * Returns the limit of the DistinctTable.
   */
  public int getLimit() {
    return _limit;
  }

  /**
   * Returns {@code true} if the DistinctTable has limit, {@code false} otherwise.
   */
  public boolean hasLimit() {
    return _limit != Integer.MAX_VALUE;
  }

  /**
   * Returns {@code true} if the DistinctTable has null handling enabled, {@code false} otherwise.
   */
  public boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  /**
   * Adds a null value into the DistinctTable.
   */
  public void addNull() {
    assert _nullHandlingEnabled;
    _hasNull = true;
    _limitWithoutNull = _limit - 1;
  }

  /**
   * Returns {@code true} if the DistinctTable has null, {@code false} otherwise.
   */
  public boolean hasNull() {
    return _hasNull;
  }

  /**
   * Returns {@code true} if the DistinctTable has order-by, {@code false} otherwise.
   */
  public abstract boolean hasOrderBy();

  /**
   * Merges another DistinctTable into the DistinctTable.
   */
  public abstract void mergeDistinctTable(DistinctTable distinctTable);

  /**
   * Merges a DataTable into the DistinctTable.
   */
  public abstract boolean mergeDataTable(DataTable dataTable);

  /**
   * Returns the number of unique rows within the DistinctTable.
   */
  public abstract int size();

  /**
   * Returns whether the DistinctTable is already satisfied.
   */
  public abstract boolean isSatisfied();

  /**
   * Returns the intermediate result as a list of rows (limit and sorting are not guaranteed).
   */
  public abstract List<Object[]> getRows();

  /**
   * Returns the intermediate result as a DataTable (limit and sorting are not guaranteed).
   */
  public abstract DataTable toDataTable()
      throws IOException;

  /**
   * Returns the final result as a ResultTable (limit applied, sorted if ordering is required).
   */
  public abstract ResultTableRows toResultTable();
}
