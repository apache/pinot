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
package org.apache.pinot.segment.spi.memory;

import java.io.DataInput;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Similar to Guava's {@link com.google.common.io.LittleEndianDataInputStream), but for Pinot.
 */
public abstract class PinotInputStream extends SeekableInputStream implements DataInput {

  public String readInt4UTF()
      throws IOException {
    int length = readInt();
    if (length == 0) {
      return StringUtils.EMPTY;
    } else {
      byte[] bytes = new byte[length];
      readFully(bytes);
      return new String(bytes, UTF_8);
    }
  }

  public abstract long availableLong();

  @Override
  public int skipBytes(int n) {
    if (n <= 0) {
      return 0;
    }
    int step = Math.min(available(), n);
    seek(getCurrentOffset() + step);
    return step;
  }

  @Override
  public int available() {
    long available = availableLong();
    if (available > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) available;
    }
  }

  @Override
  public void readFully(byte[] b)
      throws IOException {
    readFully(b, 0, b.length);
  }
}
