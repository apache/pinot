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
package org.apache.pinot.query;

import java.util.regex.Pattern;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;


public class CalciteContextExceptionClassifier {

  private static final Pattern UNKNOWN_COLUMN_PATTERN = Pattern.compile(".*Column '.+' not found in any table");
  private static final Pattern UNKNOWN_TABLE_PATTERN = Pattern.compile(".*Object '.+' not found");

  private CalciteContextExceptionClassifier() {
  }

  /// Analyzes the exception and classifies it as a [QueryErrorCode] if possible.
  ///
  /// Returns a [QueryException#QUERY_VALIDATION] exception if the exception is not recognized.
  public static QueryException classifyValidationException(CalciteContextException e) {
    String message = e.getMessage();
    if (message != null) {
      if (UNKNOWN_COLUMN_PATTERN.matcher(message).matches()) {
        return QueryErrorCode.UNKNOWN_COLUMN.asException(message, e);
      }
      if (UNKNOWN_TABLE_PATTERN.matcher(message).matches()) {
        return QueryErrorCode.TABLE_DOES_NOT_EXIST.asException(message, e);
      }
    }
    throw QueryErrorCode.QUERY_VALIDATION.asException(message, e);
  }
}
