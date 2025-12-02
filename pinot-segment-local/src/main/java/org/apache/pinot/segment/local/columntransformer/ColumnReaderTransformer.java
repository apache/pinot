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
package org.apache.pinot.segment.local.columntransformer;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;


public class ColumnReaderTransformer implements ColumnReader {

  private final List<ColumnTransformer> _transformers;
  private final ColumnReader _columnReader;

  /**
   * @param fieldSpec - The field spec for the column being created in Pinot.
   * @param columnReader - The column reader to read the source data.
   */
  public ColumnReaderTransformer(TableConfig tableConfig, Schema schema,
      FieldSpec fieldSpec, ColumnReader columnReader) {
    _columnReader = columnReader;
    _transformers = new java.util.ArrayList<>();
    addIfNotNoOp(_transformers, new DataTypeColumnTransformer(tableConfig, fieldSpec, columnReader));
    addIfNotNoOp(_transformers, new NullValueColumnTransformer(tableConfig, fieldSpec, schema));
  }

  private static void addIfNotNoOp(List<ColumnTransformer> transformers, @Nullable ColumnTransformer transformer) {
    if (transformer != null && !transformer.isNoOp()) {
      transformers.add(transformer);
    }
  }

  private Object applyTransformers(Object value) {
    for (ColumnTransformer transformer : _transformers) {
      value = transformer.transform(value);
    }
    return value;
  }

  @Override
  public boolean hasNext() {
    return _columnReader.hasNext();
  }

  @Nullable
  @Override
  public Object next()
      throws IOException {
    return applyTransformers(_columnReader.next());
  }

  @Override
  public boolean isNextNull()
      throws IOException {
    return _columnReader.isNextNull();
  }

  @Override
  public void skipNext()
      throws IOException {
    _columnReader.skipNext();
  }

  @Override
  public boolean isInt() {
    return _columnReader.isInt();
  }

  @Override
  public boolean isLong() {
    return _columnReader.isLong();
  }

  @Override
  public boolean isFloat() {
    return _columnReader.isFloat();
  }

  @Override
  public boolean isDouble() {
    return _columnReader.isDouble();
  }

  @Override
  public boolean isString() {
    return _columnReader.isString();
  }

  @Override
  public boolean isBytes() {
    return _columnReader.isBytes();
  }

  @Override
  public int nextInt()
      throws IOException {
    return _columnReader.nextInt();
  }

  @Override
  public long nextLong()
      throws IOException {
    return _columnReader.nextLong();
  }

  @Override
  public float nextFloat()
      throws IOException {
    return _columnReader.nextFloat();
  }

  @Override
  public double nextDouble()
      throws IOException {
    return _columnReader.nextDouble();
  }

  @Override
  public String nextString()
      throws IOException {
    return (String) applyTransformers(_columnReader.nextString());
  }

  @Override
  public byte[] nextBytes()
      throws IOException {
    return (byte[]) applyTransformers(_columnReader.nextBytes());
  }

  @Override
  public int[] nextIntMV()
      throws IOException {
    return _columnReader.nextIntMV();
  }

  @Override
  public long[] nextLongMV()
      throws IOException {
    return _columnReader.nextLongMV();
  }

  @Override
  public float[] nextFloatMV()
      throws IOException {
    return _columnReader.nextFloatMV();
  }

  @Override
  public double[] nextDoubleMV()
      throws IOException {
    return _columnReader.nextDoubleMV();
  }

  @Override
  public String[] nextStringMV()
      throws IOException {
    return (String[]) applyTransformers(_columnReader.nextStringMV());
  }

  @Override
  public byte[][] nextBytesMV()
      throws IOException {
    return (byte[][]) applyTransformers(_columnReader.nextBytesMV());
  }

  @Override
  public void rewind()
      throws IOException {
    _columnReader.rewind();
  }

  @Override
  public String getColumnName() {
    return _columnReader.getColumnName();
  }

  @Override
  public int getTotalDocs() {
    return _columnReader.getTotalDocs();
  }

  @Override
  public boolean isNull(int docId)
      throws IOException {
    return _columnReader.isNull(docId);
  }

  @Override
  public int getInt(int docId)
      throws IOException {
    return _columnReader.getInt(docId);
  }

  @Override
  public long getLong(int docId)
      throws IOException {
    return _columnReader.getLong(docId);
  }

  @Override
  public float getFloat(int docId)
      throws IOException {
    return _columnReader.getFloat(docId);
  }

  @Override
  public double getDouble(int docId)
      throws IOException {
    return _columnReader.getDouble(docId);
  }

  @Override
  public String getString(int docId)
      throws IOException {
    return (String) applyTransformers(_columnReader.getString(docId));
  }

  @Override
  public byte[] getBytes(int docId)
      throws IOException {
    return (byte[]) applyTransformers(_columnReader.getBytes(docId));
  }

  @Override
  public int[] getIntMV(int docId)
      throws IOException {
    return _columnReader.getIntMV(docId);
  }

  @Override
  public long[] getLongMV(int docId)
      throws IOException {
    return _columnReader.getLongMV(docId);
  }

  @Override
  public float[] getFloatMV(int docId)
      throws IOException {
    return _columnReader.getFloatMV(docId);
  }

  @Override
  public double[] getDoubleMV(int docId)
      throws IOException {
    return _columnReader.getDoubleMV(docId);
  }

  @Override
  public String[] getStringMV(int docId)
      throws IOException {
    return (String[]) applyTransformers(_columnReader.getStringMV(docId));
  }

  @Override
  public byte[][] getBytesMV(int docId)
      throws IOException {
    return (byte[][]) applyTransformers(_columnReader.getBytesMV(docId));
  }

  @Override
  public void close()
      throws IOException {
    _columnReader.close();
  }
}
