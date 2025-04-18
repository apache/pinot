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
package org.apache.pinot.segment.local.io.writer.impl;

import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.utils.BigDecimalUtils;


// used by StarTree json index
@SuppressWarnings({"unused"})
public class ZstdDataWriter implements AutoCloseable {
  private final DataOutputStream _dataOutputStream;
  private final File _file;

  public ZstdDataWriter(File file, int compressionLevel)
      throws IOException {
    _dataOutputStream = new DataOutputStream(new BufferedOutputStream(
        new ZstdOutputStream(new FileOutputStream(file), compressionLevel)
    ));
    _file = file;
  }

  public ZstdDataWriter(String fileName, int compressionLevel)
      throws IOException {
    this(fileName != null ? new File(fileName) : null, compressionLevel);
  }

  public void add(long value)
      throws IOException {
    _dataOutputStream.writeLong(value);
  }

  public void add(int value)
      throws IOException {
    _dataOutputStream.writeInt(value);
  }

  public void add(float value)
      throws IOException {
    _dataOutputStream.writeFloat(value);
  }

  public void add(double value)
      throws IOException {
    _dataOutputStream.writeDouble(value);
  }

  public void add(String value)
      throws IOException {
    if (value == null) {
      _dataOutputStream.writeInt(-1);
    } else {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      _dataOutputStream.writeInt(bytes.length);
      _dataOutputStream.write(bytes);
    }
  }

  public void add(BigDecimal value)
      throws IOException {
    BigDecimalUtils.serialize(value, _dataOutputStream);
  }

  public File getFile() {
    return _file;
  }

  @Override
  public void close()
      throws IOException {
    _dataOutputStream.flush();
    _dataOutputStream.close();
  }
}
