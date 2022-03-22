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
package org.apache.pinot.segment.spi.index.reader.provider;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public interface TextIndexReaderProvider {

  /**
   * Creates a {@see TextIndexReader}
   * @param dataBuffer the buffer, the caller is responsible for closing it
   * @param metadata the column metadata, may be used to select a reader if the buffer does not start with a magic byte.
   * @return a text index reader
   * @throws IOException if reading from the buffer fails.
   */
  TextIndexReader newFSTIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IOException;

  /**
   * Creates a {@see TextIndexReader}
   * @param file the file
   * @param metadata the column metadata, may be used to select a reader if the buffer does not start with a magic byte.
   * @return a text index reader
   */
  TextIndexReader newTextIndexReader(File file, ColumnMetadata metadata,
      @Nullable Map<String, String> textIndexProperties);
}
