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
package org.apache.pinot.spi.trace;

import java.util.Objects;

public class QueryFingerprint {
  private final String _queryHash;
  private final String _fingerprint;

  public QueryFingerprint(String queryHash, String fingerprint) {
    _queryHash = queryHash;
    _fingerprint = fingerprint;
  }

  public String getQueryHash() {
    return _queryHash;
  }

  public String getFingerprint() {
    return _fingerprint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryFingerprint that = (QueryFingerprint) o;
    return Objects.equals(_queryHash, that._queryHash) && Objects.equals(_fingerprint, that._fingerprint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_queryHash, _fingerprint);
  }

  @Override
  public String toString() {
    String hashStr = _queryHash == null ? "null" : "'" + _queryHash + "'";
    String fpStr = _fingerprint == null ? "null" : "'" + _fingerprint + "'";
    return String.format("QueryFingerprint{queryHash=%s, fingerprint=%s}", hashStr, fpStr);
  }
}
