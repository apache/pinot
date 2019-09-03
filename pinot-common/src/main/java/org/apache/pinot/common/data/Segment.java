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
package org.apache.pinot.common.data;

import javax.annotation.Nonnull;


public class Segment {
  private final String _tableNameWithType;
  private final String _segmentName;

  public Segment(@Nonnull String tableNameWithType, @Nonnull String segmentName) {
    _tableNameWithType = tableNameWithType;
    _segmentName = segmentName;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public int hashCode() {
    return 31 * _tableNameWithType.hashCode() + _segmentName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Segment) {
      Segment that = (Segment) obj;
      return _tableNameWithType.equals(that._tableNameWithType) && _segmentName.equals(that._segmentName);
    }
    return false;
  }
}
