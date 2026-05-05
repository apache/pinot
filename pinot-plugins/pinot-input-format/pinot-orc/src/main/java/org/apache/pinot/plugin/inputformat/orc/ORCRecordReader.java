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
package org.apache.pinot.plugin.inputformat.orc;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/// Record reader for ORC files. Delegates per-row extraction to [ORCRecordExtractor]; this class handles
/// file IO, column-level read filtering (for performance), and batch advance.
public class ORCRecordReader implements RecordReader {
  private static final String EXTENSION = "orc";

  private TypeDescription _orcSchema;
  private org.apache.orc.RecordReader _orcRecordReader;
  private VectorizedRowBatch _rowBatch;
  private boolean _hasNext;
  private int _nextRowId;

  private final ORCRecordExtractor _extractor = new ORCRecordExtractor();
  private final ORCRecordExtractor.Record _row = new ORCRecordExtractor.Record();

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    Configuration configuration = new Configuration();
    File orcFile = RecordReaderUtils.unpackIfRequired(dataFile, EXTENSION);
    Reader orcReader = OrcFile.createReader(new Path(orcFile.getAbsolutePath()),
        OrcFile.readerOptions(configuration).filesystem(FileSystem.getLocal(configuration)));
    _orcSchema = orcReader.getSchema();
    Preconditions.checkState(_orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "ORC schema must be of type: STRUCT");
    List<String> orcFields = _orcSchema.getFieldNames();
    List<TypeDescription> orcFieldTypes = _orcSchema.getChildren();

    // Build the file-level include array (by field id) so unread columns are skipped at the file layer.
    boolean[] orcReaderInclude = new boolean[_orcSchema.getMaximumId() + 1];
    orcReaderInclude[_orcSchema.getId()] = true;
    boolean extractAllFields = fieldsToRead == null || fieldsToRead.isEmpty();
    int numOrcFields = orcFields.size();
    for (int i = 0; i < numOrcFields; i++) {
      String field = orcFields.get(i);
      if (extractAllFields || fieldsToRead.contains(field)) {
        initFieldsToRead(orcReaderInclude, orcFieldTypes.get(i), field);
      }
    }

    _orcRecordReader = orcReader.rows(new Reader.Options().include(orcReaderInclude));
    _rowBatch = _orcSchema.createRowBatch();
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;

    _extractor.init(fieldsToRead, null);
  }

  /// Recursively walks `fieldType`, marking every field id (including children of `LIST` / `MAP` / `STRUCT`)
  /// in `orcReaderInclude` so the underlying ORC reader populates those columns. Validates that single-value
  /// fields and map keys use a category supported by [ORCRecordExtractor].
  private void initFieldsToRead(boolean[] orcReaderInclude, TypeDescription fieldType, String field) {
    int fieldId = fieldType.getId();
    orcReaderInclude[fieldId] = true;
    TypeDescription.Category category = fieldType.getCategory();
    if (category == TypeDescription.Category.LIST) {
      initFieldsToRead(orcReaderInclude, fieldType.getChildren().get(0), field);
    } else if (category == TypeDescription.Category.MAP) {
      // Include the map's keys column too.
      orcReaderInclude[fieldId + 1] = true;
      List<TypeDescription> children = fieldType.getChildren();
      TypeDescription.Category keyCategory = children.get(0).getCategory();
      Preconditions.checkState(isSupportedSingleValueType(keyCategory), "Illegal map key field type: %s (field %s)",
          keyCategory, field);
      initFieldsToRead(orcReaderInclude, children.get(1), field);
    } else if (category == TypeDescription.Category.STRUCT) {
      List<String> childrenFieldNames = fieldType.getFieldNames();
      List<TypeDescription> childrenFieldTypes = fieldType.getChildren();
      for (int i = 0; i < childrenFieldNames.size(); i++) {
        initFieldsToRead(orcReaderInclude, childrenFieldTypes.get(i), childrenFieldNames.get(i));
      }
    } else {
      Preconditions.checkState(isSupportedSingleValueType(category), "Illegal single-value field type: %s (field %s)",
          category, field);
    }
  }

  private static boolean isSupportedSingleValueType(TypeDescription.Category category) {
    switch (category) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case BINARY:
      case DECIMAL:
      case VARCHAR:
      case CHAR:
      case TIMESTAMP_INSTANT:
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _row.set(_rowBatch, _orcSchema, _nextRowId);
    _extractor.extract(_row, reuse);

    // Advance, fetching the next batch when the current one is exhausted.
    if (_nextRowId == _rowBatch.size - 1) {
      try {
        _hasNext = _orcRecordReader.nextBatch(_rowBatch);
      } catch (IOException e) {
        throw new RecordFetchException("Failed to read next ORC record", e);
      }
      _nextRowId = 0;
    } else {
      _nextRowId++;
    }
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _orcRecordReader.seekToRow(0);
    _hasNext = _orcRecordReader.nextBatch(_rowBatch);
    _nextRowId = 0;
  }

  @Override
  public void close()
      throws IOException {
    _orcRecordReader.close();
  }
}
