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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The ORCRecordReader uses a VectorizedRowBatch, which we convert to a Writable. Then, we convert these
 * Writable objects to primitives that we can then store in a GenericRow.
 *
 * When new data types are added to Pinot, we will need to update them here as well.
 * Note that not all ORC types are supported; we only support the ORC types that correspond to either
 * primitives or multivalue columns in Pinot, which is similar to other record readers.
 */
public class ORCRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ORCRecordReader.class);
  private static final String LOCAL_FS_PREFIX = "file://";

  private Schema _pinotSchema;
  private ORCRecordExtractor _recordExtractor;
  private TypeDescription _orcSchema;
  private Reader _reader;
  private org.apache.orc.RecordReader _recordReader;
  private VectorizedRowBatch _reusableVectorizedRowBatch;

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _pinotSchema = schema;
    Path dataFilePath = new Path(LOCAL_FS_PREFIX + dataFile.getAbsolutePath());
    LOGGER.info("Creating segment from path: {}", dataFilePath);
    _reader = OrcFile.createReader(dataFilePath, OrcFile.readerOptions(new Configuration()));
    _orcSchema = _reader.getSchema();
    LOGGER.info("ORC schema: {}", _orcSchema.toJson());
    _recordReader = _reader.rows(_reader.options().schema(_orcSchema));

    // Create a row batch with max size 1
    _reusableVectorizedRowBatch = _orcSchema.createRowBatch(1);
    List<String> sourceFields = new ArrayList<>(SchemaFieldExtractorUtils.extract(schema));
    ORCRecordExtractorConfig recordExtractorConfig = new ORCRecordExtractorConfig();
    recordExtractorConfig.setOrcSchema(_orcSchema);
    _recordExtractor = new ORCRecordExtractor();
    _recordExtractor.init(sourceFields, recordExtractorConfig);
  }

  @Override
  public boolean hasNext() {
    try {
      return _recordReader.getProgress() != 1;
    } catch (IOException e) {
      LOGGER.error("Could not get next record");
      throw new RuntimeException(e);
    }
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    _recordReader.nextBatch(_reusableVectorizedRowBatch);
    return _recordExtractor.extract(_reusableVectorizedRowBatch, reuse);
  }


  @Override
  public void rewind()
      throws IOException {
    _recordReader = _reader.rows();
  }

  @Override
  public Schema getSchema() {
    return _pinotSchema;
  }

  @Override
  public void close()
      throws IOException {
    _recordReader.close();
  }
}
