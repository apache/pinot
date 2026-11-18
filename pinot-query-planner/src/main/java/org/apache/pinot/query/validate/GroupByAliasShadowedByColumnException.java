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
package org.apache.pinot.query.validate;

import org.apache.pinot.spi.exception.QueryException;


/// A [QueryException] with [org.apache.pinot.spi.exception.QueryErrorCode#QUERY_VALIDATION] for a query that failed
/// validation and has a `GROUP BY` name that matches a `SELECT` alias and a column. The validator resolved the name to
/// the column, because `preferSelectAliasInGroupBy` was not set. With the option set, the same query can validate.
///
/// It has the same error code, message and cause as the exception it replaces, so code that handles a
/// [QueryException] treats both the same way. The controller uses it to retry the query with the option set. See
/// `PinotQueryResource#getTableNames`.
public class GroupByAliasShadowedByColumnException extends QueryException {

  public GroupByAliasShadowedByColumnException(QueryException validationException) {
    super(validationException.getErrorCode(), validationException.getMessage(), validationException.getCause());
    setStackTrace(validationException.getStackTrace());
  }
}
