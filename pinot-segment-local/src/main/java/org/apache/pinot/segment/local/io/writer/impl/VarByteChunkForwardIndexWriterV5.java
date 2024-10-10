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

import java.io.File;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;


/**
 * Forward index writer that extends {@link VarByteChunkForwardIndexWriterV4} with the only difference being the
 * version tag is now bumped from 4 to 5.
 *
 * <p>The {@code VERSION} tag is a {@code static final} class variable set to {@code 5}. Since static variables
 * are shadowed in the child class thus associated with the class that defines them, care must be taken to ensure
 * that the parent class can correctly observe the child class's {@code VERSION} value at runtime.</p>
 *
 * <p>To achieve this, the {@code getVersion()} method is overridden to return the concrete subclass's
 * {@code VERSION} value, ensuring that the correct version number is returned even when using a reference
 * to the parent class.</p>
 *
 * @see VarByteChunkForwardIndexWriterV4
 * @see VarByteChunkForwardIndexWriterV5#getVersion()
 */
@NotThreadSafe
public class VarByteChunkForwardIndexWriterV5 extends VarByteChunkForwardIndexWriterV4 {
  public static final int VERSION = 5;

  public VarByteChunkForwardIndexWriterV5(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    super(file, compressionType, chunkSize);
  }

  @Override
  public int getVersion() {
    return VERSION;
  }
}
