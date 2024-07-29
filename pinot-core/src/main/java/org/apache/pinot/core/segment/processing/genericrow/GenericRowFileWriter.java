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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * File writer for {@link GenericRow}. The writer will generate to 2 files, one for the offsets (BIG_ENDIAN) and one for
 * the actual data (NATIVE_ORDER). The generated files can be read by the {@link GenericRowFileReader}. There is no
 * version control for the files generated because the files should only be used as intermediate format and read in the
 * same host (different host might have different NATIVE_ORDER).
 *
 * TODO: Consider using ByteBuffer instead of OutputStream.
 */
public class GenericRowFileWriter implements Closeable, FileWriter<GenericRow> {
  private final DataOutputStream _offsetStream;
  private final BufferedOutputStream _dataStream;
  private final GenericRowSerializer _serializer;

  private long _nextOffset;

  public GenericRowFileWriter(File offsetFile, File dataFile, List<FieldSpec> fieldSpecs, boolean includeNullFields)
      throws FileNotFoundException {
    _offsetStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(offsetFile)));
    _dataStream = new BufferedOutputStream(new FileOutputStream(dataFile));
    _serializer = new GenericRowSerializer(fieldSpecs, includeNullFields);
  }

  /**
   * Writes the given row into the files.
   */
  public void write(GenericRow genericRow)
      throws IOException {
    _offsetStream.writeLong(_nextOffset);
    byte[] bytes = _serializer.serialize(genericRow);
    _dataStream.write(bytes);
    _nextOffset += bytes.length;
  }

  public long writeData(GenericRow genericRow)
      throws IOException {
    _offsetStream.writeLong(_nextOffset);
    byte[] bytes = _serializer.serialize(genericRow);
    _dataStream.write(bytes);
    _nextOffset += bytes.length;
    return bytes.length;
  }

  @Override
  public void close()
      throws IOException {
    try {
      // Wrapping around try block to make sure dataStream is closed, despite failures while closing offsetStream.
      _offsetStream.close();
    } finally {
      _dataStream.close();
    }
  }
}
