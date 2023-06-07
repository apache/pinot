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
package org.apache.pinot.core.segment.processing.reducer;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DedupReducer deduplicates the GenericRows with the same values.
 */
public class DedupReducer implements Reducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(DedupReducer.class);

  private final String _partitionId;
  private final GenericRowFileManager _fileManager;
  private final File _reducerOutputDir;
  private GenericRowFileManager _dedupFileManager;

  public DedupReducer(String partitionId, GenericRowFileManager fileManager, File reducerOutputDir) {
    _partitionId = partitionId;
    _fileManager = fileManager;
    _reducerOutputDir = reducerOutputDir;
  }

  @Override
  public GenericRowFileManager reduce()
      throws Exception {
    try {
      return doReduce();
    } catch (Exception e) {
      // Cleaning up resources created by the reducer, leaving others to the caller like the input _fileManager.
      if (_dedupFileManager != null) {
        _dedupFileManager.cleanUp();
      }
      throw e;
    }
  }

  private GenericRowFileManager doReduce()
      throws Exception {
    LOGGER.info("Start reducing on partition: {}", _partitionId);
    long reduceStartTimeMs = System.currentTimeMillis();

    GenericRowFileReader fileReader = _fileManager.getFileReader();
    int numRows = fileReader.getNumRows();
    int numSortFields = fileReader.getNumSortFields();
    LOGGER.info("Start sorting on numRows: {}, numSortFields: {}", numRows, numSortFields);
    long sortStartTimeMs = System.currentTimeMillis();
    GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
    LOGGER.info("Finish sorting in {}ms", System.currentTimeMillis() - sortStartTimeMs);

    File partitionOutputDir = new File(_reducerOutputDir, _partitionId);
    FileUtils.forceMkdir(partitionOutputDir);
    LOGGER.info("Start creating dedup file under dir: {}", partitionOutputDir);
    long dedupFileCreationStartTimeMs = System.currentTimeMillis();
    _dedupFileManager =
        new GenericRowFileManager(partitionOutputDir, _fileManager.getFieldSpecs(), _fileManager.isIncludeNullFields(),
            0);
    GenericRowFileWriter dedupFileWriter = _dedupFileManager.getFileWriter();
    GenericRow previousRow = new GenericRow();
    recordReader.read(0, previousRow);
    int previousRowId = 0;
    dedupFileWriter.write(previousRow);
    for (int i = 1; i < numRows; i++) {
      if (recordReader.compare(previousRowId, i) != 0) {
        previousRow.clear();
        recordReader.read(i, previousRow);
        previousRowId = i;
        dedupFileWriter.write(previousRow);
      }
    }
    _dedupFileManager.closeFileWriter();
    LOGGER.info("Finish creating dedup file in {}ms", System.currentTimeMillis() - dedupFileCreationStartTimeMs);

    _fileManager.cleanUp();
    LOGGER.info("Finish reducing in {}ms", System.currentTimeMillis() - reduceStartTimeMs);
    return _dedupFileManager;
  }
}
