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
package org.apache.pinot.segment.spi.index.startree;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


public class AggregationSpec {
  public static final AggregationSpec DEFAULT = new AggregationSpec(ChunkCompressionType.PASS_THROUGH);

  private final ChunkCompressionType _compressionType;

  public AggregationSpec(ChunkCompressionType compressionType) {
    _compressionType = compressionType;
  }

  public ChunkCompressionType getCompressionType() {
    return _compressionType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AggregationSpec)) {
      return false;
    }
    AggregationSpec that = (AggregationSpec) o;
    return _compressionType == that._compressionType;
  }

  @Override
  public int hashCode() {
    return _compressionType.hashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("compressionType", _compressionType)
        .toString();
  }
}
