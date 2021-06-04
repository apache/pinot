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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Manager for generic row files.
 */
public class GenericRowFileManager {
  public static final String OFFSET_FILE_NAME = "record.offset";
  public static final String DATA_FILE_NAME = "record.data";

  private final File _offsetFile;
  private final File _dataFile;
  private final List<FieldSpec> _fieldSpecs;
  private final boolean _includeNullFields;

  private GenericRowFileWriter _fileWriter;
  private GenericRowFileReader _fileReader;

  public GenericRowFileManager(File outputDir, List<FieldSpec> fieldSpecs, boolean includeNullFields) {
    _offsetFile = new File(outputDir, OFFSET_FILE_NAME);
    _dataFile = new File(outputDir, DATA_FILE_NAME);
    _fieldSpecs = fieldSpecs;
    _includeNullFields = includeNullFields;
  }

  /**
   * Returns the file writer. Creates one if not exists.
   */
  public GenericRowFileWriter getFileWriter()
      throws IOException {
    if (_fileWriter == null) {
      Preconditions.checkState(!_offsetFile.exists(), "Record offset file: %s already exists", _offsetFile);
      Preconditions.checkState(!_dataFile.exists(), "Record data file: %s already exists", _dataFile);
      _fileWriter = new GenericRowFileWriter(_offsetFile, _dataFile, _fieldSpecs, _includeNullFields);
    }
    return _fileWriter;
  }

  /**
   * Closes the file writer.
   */
  public void closeFileWriter()
      throws IOException {
    if (_fileWriter != null) {
      _fileWriter.close();
      _fileWriter = null;
    }
  }

  /**
   * Returns the file reader. Creates one if not exists.
   */
  public GenericRowFileReader getFileReader()
      throws IOException {
    if (_fileReader == null) {
      Preconditions.checkState(_offsetFile.exists(), "Record offset file: %s does not exist", _offsetFile);
      Preconditions.checkState(_dataFile.exists(), "Record data file: %s does not exist", _dataFile);
      _fileReader = new GenericRowFileReader(_offsetFile, _dataFile, _fieldSpecs, _includeNullFields);
    }
    return _fileReader;
  }

  /**
   * Closes the file reader.
   */
  public void closeFileReader()
      throws IOException {
    if (_fileReader != null) {
      _fileReader.close();
      _fileReader = null;
    }
  }

  /**
   * Cleans up the files.
   */
  public void cleanUp()
      throws IOException {
    closeFileWriter();
    closeFileReader();
    FileUtils.deleteQuietly(_offsetFile);
    FileUtils.deleteQuietly(_dataFile);
  }
}
