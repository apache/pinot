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
package org.apache.pinot.orc.data.readers;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.data.readers.RecordReaderConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
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
  private TypeDescription _orcSchema;
  private Reader _reader;
  private org.apache.orc.RecordReader _recordReader;
  private VectorizedRowBatch _reusableVectorizedRowBatch;

  private void init(String inputPath, Schema schema)
      throws IOException {
    _pinotSchema = schema;
    Path dataFilePath = new Path(LOCAL_FS_PREFIX + inputPath);
    LOGGER.info("Creating segment from path: {}", dataFilePath);
    _reader = OrcFile.createReader(dataFilePath, OrcFile.readerOptions(new Configuration()));
    _orcSchema = _reader.getSchema();
    LOGGER.info("ORC schema: {}", _orcSchema.toJson());
    _recordReader = _reader.rows(_reader.options().schema(_orcSchema));

    // Create a row batch with max size 1
    _reusableVectorizedRowBatch = _orcSchema.createRowBatch(1);
  }

  @Override
  public void init(String inputPath, Schema schema, RecordReaderConfig recordReaderConfig)
      throws IOException {
    init(inputPath, schema);
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
    fillGenericRow(reuse, _reusableVectorizedRowBatch);
    return reuse;
  }

  private void fillGenericRow(GenericRow genericRow, VectorizedRowBatch rowBatch) {
    // TODO: use Pinot schema to fill the values to handle missing column and default values properly

    // ORC's TypeDescription is the equivalent of a schema. The way we will support ORC in Pinot
    // will be to get the top level struct that contains all our fields and look through its
    // children to determine the fields in our schemas.
    if (_orcSchema.getCategory().equals(TypeDescription.Category.STRUCT)) {
      for (int i = 0; i < _orcSchema.getChildren().size(); i++) {
        // Get current column in schema
        TypeDescription currColumn = _orcSchema.getChildren().get(i);
        String currColumnName = _orcSchema.getFieldNames().get(i);
        if (!_pinotSchema.getColumnNames().contains(currColumnName)) {
          LOGGER.warn("Skipping column {} because it is not in pinot schema", currColumnName);
          continue;
        }
        // ORC will keep your columns in the same order as the schema provided
        ColumnVector vector = rowBatch.cols[i];
        // Previous value set to null, not used except to save allocation memory in OrcMapredRecordReader
        WritableComparable writableComparable;
        writableComparable = OrcMapredRecordReader.nextValue(vector, 0, currColumn, null);
        genericRow.putField(currColumnName, getBaseObject(writableComparable));
      }
    } else {
      throw new IllegalArgumentException("Not a valid schema");
    }
  }

  /**
   * A utility method to convert an Orc WritableComparable object to a generic Java object that can
   * be added to a Pinot GenericRow object
   *
   * @param w Orc WritableComparable to convert
   * @return Object that will be added to the Pinot GenericRow
   */
  private Object getBaseObject(WritableComparable w) {
    Object obj = null;

    if (w == null || NullWritable.class.isAssignableFrom(w.getClass())) {
      obj = null;
    } else if (BooleanWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((BooleanWritable) w).get();
    } else if (ByteWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((ByteWritable) w).get();
    } else if (ShortWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((ShortWritable) w).get();
    } else if (IntWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((IntWritable) w).get();
    } else if (LongWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((LongWritable) w).get();
    } else if (FloatWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((FloatWritable) w).get();
    } else if (DoubleWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((DoubleWritable) w).get();
    } else if (BytesWritable.class.isAssignableFrom(w.getClass())) {
      obj = ((BytesWritable) w).getBytes();
    } else if (Text.class.isAssignableFrom(w.getClass())) {
      obj = ((Text) w).toString();
    } else if (OrcList.class.isAssignableFrom(w.getClass())) {
      obj = translateList((OrcList) w);
    } else {
      LOGGER.info("Unknown type found: " + w.getClass().getSimpleName());
      throw new IllegalArgumentException("Unknown type: " + w.getClass().getSimpleName());
    }

    return obj;
  }

  private List<Object> translateList(OrcList<? extends WritableComparable> l) {
    if (l == null || l.isEmpty()) {
      return ImmutableList.of();
    }

    List<Object> retArray = new ArrayList<>(l.size());

    for (WritableComparable w : l) {
      Object o = getBaseObject(w);
      retArray.add(o);
    }
    return retArray;
  }

  @Override
  public void rewind()
      throws IOException {
    _recordReader = _reader.rows();
  }

  @Override
  public org.apache.pinot.common.data.Schema getSchema() {
    return _pinotSchema;
  }

  @Override
  public void close()
      throws IOException {
    _recordReader.close();
  }
}
