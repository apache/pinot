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
package org.apache.pinot.core.segment.processing.genericrow;

import java.io.IOException;
import org.apache.pinot.spi.data.readers.GenericRow;


public class AdaptiveSizeBasedWriter implements AdaptiveConstraintsWriter<GenericRowFileWriter, GenericRow> {

  private final long _bytesLimit;
  private long _numBytesWritten;

  public AdaptiveSizeBasedWriter(long bytesLimit) {
    _bytesLimit = bytesLimit;
    _numBytesWritten = 0;
  }

  public long getBytesLimit() {
    return _bytesLimit;
  }
  public long getNumBytesWritten() {
    return _numBytesWritten;
  }

  @Override
  public boolean canWrite() {
    return _numBytesWritten < _bytesLimit;
  }

  @Override
  public void write(GenericRowFileWriter writer, GenericRow row) throws IOException {
    _numBytesWritten += writer.writeData(row);
  }
}
