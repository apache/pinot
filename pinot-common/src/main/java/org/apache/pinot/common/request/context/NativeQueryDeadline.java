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
package org.apache.pinot.common.request.context;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;


/// Immutable end-to-end query time bound in an explicitly qualified shared clock domain. The domain identifies the
/// deployment's time scale, leap/smear convention and qualification contract; unrelated monotonic clock origins are
/// never interchangeable. Fanout, transit and retries must preserve this value, including its original lower bound.
/// This is a bounded deadline, not authentication or serving authority.
public final class NativeQueryDeadline {
  private final String _clockDomain;
  private final long _originLowerMillis;
  private final long _notAfterMillis;

  public NativeQueryDeadline(String clockDomain, long originLowerMillis, long notAfterMillis) {
    checkArgument(clockDomain != null && !clockDomain.isEmpty() && clockDomain.length() <= 256
        && clockDomain.chars().allMatch(c -> c >= 33 && c <= 126), "Invalid native query clock domain");
    checkArgument(originLowerMillis >= 0 && notAfterMillis > originLowerMillis,
        "Invalid native query deadline interval");
    _clockDomain = clockDomain;
    _originLowerMillis = originLowerMillis;
    _notAfterMillis = notAfterMillis;
  }

  public String clockDomain() {
    return _clockDomain;
  }

  public long originLowerMillis() {
    return _originLowerMillis;
  }

  public long notAfterMillis() {
    return _notAfterMillis;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof NativeQueryDeadline)) {
      return false;
    }
    NativeQueryDeadline that = (NativeQueryDeadline) other;
    return _clockDomain.equals(that._clockDomain) && _originLowerMillis == that._originLowerMillis
        && _notAfterMillis == that._notAfterMillis;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_clockDomain, _originLowerMillis, _notAfterMillis);
  }
}
