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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extractor for ORC records
 */
public class ORCRecordExtractor implements RecordExtractor<VectorizedRowBatch> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ORCRecordExtractor.class);

  private TypeDescription _orcSchema;

  @Override
  public void init(RecordExtractorConfig recordExtractorConfig) {
    _orcSchema = ((ORCRecordExtractorConfig) recordExtractorConfig).getOrcSchema();
  }

  @Override
  public GenericRow extract(List<String> sourceFieldNames, VectorizedRowBatch from, GenericRow to) {
    // TODO: use Pinot schema to fill the values to handle missing column and default values properly

    // ORC's TypeDescription is the equivalent of a schema. The way we will support ORC in Pinot
    // will be to get the top level struct that contains all our fields and look through its
    // children to determine the fields in our schemas.
    if (_orcSchema.getCategory().equals(TypeDescription.Category.STRUCT)) {
      List<TypeDescription> orcSchemaChildren = _orcSchema.getChildren();
      for (int i = 0; i < orcSchemaChildren.size(); i++) {
        // Get current column in schema
        String currColumnName = _orcSchema.getFieldNames().get(i);
        if (!sourceFieldNames.contains(currColumnName)) {
          LOGGER.warn("Skipping column {} because it is not in source columns", currColumnName);
          continue;
        }
        // ORC will keep your columns in the same order as the schema provided
        ColumnVector vector = from.cols[i];
        // Previous value set to null, not used except to save allocation memory in OrcMapredRecordReader
        TypeDescription currColumn = orcSchemaChildren.get(i);
        WritableComparable writableComparable = OrcMapredRecordReader.nextValue(vector, 0, currColumn, null);
        to.putValue(currColumnName, getBaseObject(writableComparable));
      }
    } else {
      throw new IllegalArgumentException("Not a valid schema");
    }
    return to;
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
      obj = w.toString();
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
}
