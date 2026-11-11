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
package org.apache.pinot.segment.local.io.codec;

import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Context supplied to [CodecDefinition#validateContext()] during pipeline validation.
///
/// Carries the column's stored data type so that codecs can reject unsupported types at
/// configuration time rather than at runtime. Instances are immutable and thread-safe.
final class CodecContext {
  private final DataType _dataType;

  CodecContext(DataType dataType) {
    _dataType = dataType;
  }

  /// Returns the stored [DataType] of the column being indexed.
  DataType getDataType() {
    return _dataType;
  }
}
