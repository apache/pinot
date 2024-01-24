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
package org.apache.pinot.segment.spi.compression;

/**
 * Compression type for dictionary-encoded forward index, where the values stored are dictionary ids.
 */
public enum DictIdCompressionType {
  // Add a second level dictionary encoding for the multi-value entries
  MV_ENTRY_DICT(false, true);

  private final boolean _applicableToSV;
  private final boolean _applicableToMV;

  DictIdCompressionType(boolean applicableToSV, boolean applicableToMV) {
    _applicableToSV = applicableToSV;
    _applicableToMV = applicableToMV;
  }

  public boolean isApplicableToSV() {
    return _applicableToSV;
  }

  public boolean isApplicableToMV() {
    return _applicableToMV;
  }

  public boolean isApplicable(boolean isSingleValue) {
    return isSingleValue ? isApplicableToSV() : isApplicableToMV();
  }
}
