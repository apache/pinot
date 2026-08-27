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
package org.apache.pinot.query.planner.spi.stats;


/// Checked exception thrown by [ColumnStatsSource] when column statistics cannot be fetched and no
/// partial result can be returned.
///
/// Throwing is the last resort: a source that obtained statistics for some segments should return
/// them and omit the rest, since a partial result still improves estimates. Callers must treat this
/// as "no new statistics this round" and degrade to whatever is already stored — collection failures
/// must never fail a query.
///
/// Thread-safety: exception objects are not shared; no concurrency requirements.
public class ColumnStatsFetchException extends Exception {

  /// Constructs a new [ColumnStatsFetchException] with the given message.
  ///
  /// @param message description of the error
  public ColumnStatsFetchException(String message) {
    super(message);
  }

  /// Constructs a new [ColumnStatsFetchException] with the given message and cause.
  ///
  /// @param message description of the error
  /// @param cause   the underlying cause
  public ColumnStatsFetchException(String message, Throwable cause) {
    super(message, cause);
  }
}
