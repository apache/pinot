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
package org.apache.pinot.segment.local.io.reader.impl;

import com.github.luben.zstd.ZstdInputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


// used by StarTree json index
@SuppressWarnings({"unused"})
public class ZstdDataReader implements AutoCloseable {
  private final DataInputStream _dataInputStream;

  public ZstdDataReader(File file)
      throws IOException {
    _dataInputStream = new DataInputStream(new BufferedInputStream(
        new ZstdInputStream(new FileInputStream(file))
    ));
  }

  public ZstdDataReader(String fileName)
      throws IOException {
    this(fileName != null ? new File(fileName) : null);
  }

  public long nextLong()
      throws IOException {
    return _dataInputStream.readLong();
  }

  public int nextInt()
      throws IOException {
    return _dataInputStream.readInt();
  }

  public float nextFloat()
      throws IOException {
    return _dataInputStream.readFloat();
  }

  public double nextDouble()
      throws IOException {
    return _dataInputStream.readDouble();
  }

  public String nextString()
      throws IOException {
    int length = _dataInputStream.readInt();
    if (length == -1) {
      return null;
    }
    byte[] bytes = new byte[length];
    _dataInputStream.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public ByteArray nextByteArray()
      throws IOException {
    int length = _dataInputStream.readInt();
    if (length == -1) {
      return null;
    }
    byte[] bytes = new byte[length];
    _dataInputStream.readFully(bytes);
    return new ByteArray(bytes);
  }

  public boolean hasNext()
      throws IOException {
    return _dataInputStream.available() > 0;
  }

  public BigDecimal nextBigDecimal()
      throws IOException {
    return BigDecimalUtils.deserialize(_dataInputStream);
  }

  @Override
  public void close()
      throws IOException {
    _dataInputStream.close();
  }
}
