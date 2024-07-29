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
package org.apache.pinot.controller.recommender.data.writer;

import java.io.File;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;
import org.apache.pinot.spi.data.Schema;


public class AvroWriterSpec extends WriterSpec {

  private final File _baseDir;
  private final long _totalDocs;
  private final int _numFiles;
  private final Schema _schema;

  public AvroWriterSpec(DataGenerator generator, File baseDir, long totalDocs, int numFiles) {
    super(generator);
    _baseDir = baseDir;
    _totalDocs = totalDocs;
    _numFiles = numFiles;
    _schema = generator.fetchSchema();
  }

  public Schema getSchema() {
    return _schema;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public int getNumFiles() {
    return _numFiles;
  }

  public File getBaseDir() {
    return _baseDir;
  }
}
