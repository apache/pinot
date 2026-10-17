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
package org.apache.pinot.common.metadata.columndeletion;


/// Thrown when a deletion-ledger znode uses a format version this process cannot read or write.
///
/// Mixed-version controllers must fail closed rather than clobber a newer record.
public final class ColumnDeletionUnsupportedFormatException extends IllegalArgumentException {
  private final int _formatVersion;
  private final int _supportedFormatVersion;

  public ColumnDeletionUnsupportedFormatException(int formatVersion, int supportedFormatVersion) {
    super("Column deletion metadata format version " + formatVersion + " is newer than supported version "
        + supportedFormatVersion);
    _formatVersion = formatVersion;
    _supportedFormatVersion = supportedFormatVersion;
  }

  public int getFormatVersion() {
    return _formatVersion;
  }

  public int getSupportedFormatVersion() {
    return _supportedFormatVersion;
  }
}
