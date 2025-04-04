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
package org.apache.pinot.common.compression;

import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class DeflateCompressor implements Compressor {
  public static final DeflateCompressor INSTANCE = new DeflateCompressor();

  @Override
  public byte[] compress(byte[] input)
      throws Exception {
    Deflater deflater = new Deflater();
    deflater.setInput(input);
    deflater.finish();
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    while (!deflater.finished()) {
      int count = deflater.deflate(buffer);
      bos.write(buffer, 0, count);
    }
    return bos.toByteArray();
  }

  @Override
  public byte[] decompress(byte[] input)
      throws Exception {
    Inflater inflater = new Inflater();
    inflater.setInput(input);
    byte[] buffer = new byte[1024];
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    while (!inflater.finished()) {
      int count = inflater.inflate(buffer);
      bos.write(buffer, 0, count);
    }
    return bos.toByteArray();
  }
}
