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
package org.apache.pinot.core.common.datatable;

import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;


/// Thrown by a [DataTableBuilder] when the rows written so far are larger than the maximum response size of the query.
/// The builder throws it while it builds the response, so the server stops before it holds the full response.
public class ResponseSizeLimitExceededException extends QueryException {
  private final long _sizeBytes;

  public ResponseSizeLimitExceededException(long sizeBytes, long maxSizeBytes) {
    super(QueryErrorCode.QUERY_CANCELLATION,
        "Serialized query response size of at least " + sizeBytes + " exceeds threshold " + maxSizeBytes);
    _sizeBytes = sizeBytes;
  }

  /// Returns the bytes written when the builder stopped, which is a lower bound of the size of the response.
  public long getSizeBytes() {
    return _sizeBytes;
  }
}
