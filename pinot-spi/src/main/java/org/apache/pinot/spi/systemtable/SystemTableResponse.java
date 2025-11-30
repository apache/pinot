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
package org.apache.pinot.spi.systemtable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Response returned by {@link SystemTableProvider} containing materialized rows.
 */
public final class SystemTableResponse {
  private final List<GenericRow> _rows;
  private final long _refreshedAtMillis;
  private final int _totalRows;

  public SystemTableResponse(List<GenericRow> rows, long refreshedAtMillis, int totalRows) {
    _rows = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rows, "rows must not be null")));
    _refreshedAtMillis = refreshedAtMillis;
    _totalRows = totalRows;
  }

  public List<GenericRow> getRows() {
    return _rows;
  }

  /**
   * Epoch millis when the data backing the response was refreshed.
   */
  public long getRefreshedAtMillis() {
    return _refreshedAtMillis;
  }

  /**
   * Total number of rows before applying offset/limit.
   */
  public int getTotalRows() {
    return _totalRows;
  }
}
