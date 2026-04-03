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
package org.apache.pinot.plugin.inputformat.arrow;

import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Config for {@link ArrowRecordReader}.
 */
public class ArrowRecordReaderConfig implements RecordReaderConfig {
  public static final long DEFAULT_ALLOCATOR_LIMIT = 268435456L; // 256MB

  private long _allocatorLimit = DEFAULT_ALLOCATOR_LIMIT;

  public ArrowRecordReaderConfig() {
  }

  public long getAllocatorLimit() {
    return _allocatorLimit;
  }

  public void setAllocatorLimit(long allocatorLimit) {
    _allocatorLimit = allocatorLimit;
  }
}
