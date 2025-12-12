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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.columntransformer.ColumnTransformer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;


/**
 * A decorator for {@link ColumnReader} that applies a chain of {@link ColumnTransformer}s to column data during
 * ingestion. This class serves as the primary mechanism for transforming raw input data into Pinot's internal
 * representation while maintaining optimal performance.
 *
 * <h3>Role and Responsibilities</h3>
 * <p>
 * The ColumnReaderTransformer wraps a source {@link ColumnReader} and transparently applies transformations to
 * values as they are read. Built-in transformers include:
 * <ul>
 *   <li>{@link DataTypeColumnTransformer} - Handles data type conversions and validations</li>
 *   <li>{@link NullValueColumnTransformer} - Manages null value handling and default value substitution</li>
 *   <li>Additional custom transformers - User-defined transformations for specific use cases</li>
 * </ul>
 * </p>
 *
 * <h3>Performance Optimization Strategy</h3>
 * <p>
 * This class implements several critical performance optimizations to minimize overhead during data ingestion:
 * </p>
 *
 * <h4>1. Transformer Segregation</h4>
 * <p>
 * The transformers are divided into two groups:
 * <ul>
 *   <li><b>_allTransformers</b> - All transformers including {@link NullValueColumnTransformer}</li>
 *   <li><b>_allTransformersExceptNullTransformer</b> - All transformers except {@link NullValueColumnTransformer}</li>
 * </ul>
 * This segregation is necessary because {@link NullValueColumnTransformer} requires boxing primitive types to
 * {@link Object}, which introduces significant performance overhead. By separating it, we can check if any other
 * transformations are needed. If not, we can bypass transformation logic entirely for primitive types.
 * </p>
 *
 * <h4>2. Type-Specific Optimization</h4>
 * <p>
 * The class optimizes differently based on data type:
 * <ul>
 *   <li><b>Primitive numeric types (int, long, float, double)</b> - Can be used when transform isn't required.</li>
 * </ul>
 * </p>
 *
 * <h4>3. Fast Path for Zero Transformers</h4>
 * <p>
 * When {@code _allTransformersExceptNullTransformer} is empty (common for primitive types with no transformations),
 * all primitive accessor methods (e.g., {@link #nextInt()}, {@link #getLong(int)}) bypass transformation logic
 * entirely and delegate directly to the underlying reader. This ensures zero overhead when no transformations are
 * actually needed.
 * </p>
 *
 * <h3>Null Value Handling</h3>
 * <p>
 * Special attention is paid to null value detection in the {@link #isNull(int)} method. This method must return
 * {@code true} not only when the source value is null, but also when any transformer would produce a null result.
 * This ensures that null vector indexes are built correctly even when transformers modify null semantics.
 * However, the {@link NullValueColumnTransformer} itself is excluded from this check since it converts nulls
 * to default values - we still want to track the original null state for index building purposes.
 * </p>
 *
 * @see ColumnReader
 * @see ColumnTransformer
 * @see DataTypeColumnTransformer
 * @see NullValueColumnTransformer
 */
public class ColumnReaderTransformer implements ColumnReader {

  private final List<ColumnTransformer> _allTransformers;

  // Transformers except NullValueColumnTransformer
  // Since NullValueColumnTransformer is required for all columns, but has perf overhead (casting to Object),
  // we separate it out to avoid calling it unless necessary.
  private final List<ColumnTransformer> _allTransformersExceptNullTransformer;

  private final ColumnTransformer _nullValueTransformer;
  private final ColumnReader _columnReader;

  /**
   * Creates a ColumnReaderTransformer with only built-in transformers (no additional custom transformers).
   *
   * @param fieldSpec The field specification for the column being transformed and created in Pinot
   * @param columnReader The source column reader to read raw data from
   */
  public ColumnReaderTransformer(TableConfig tableConfig, Schema schema,
      FieldSpec fieldSpec, ColumnReader columnReader) {
    this(tableConfig, schema, fieldSpec, columnReader, new ArrayList<>());
  }

  /**
   * Creates a ColumnReaderTransformer with both built-in and additional custom transformers.
   * The additional transformers are applied last in the order provided
   *
   * @param fieldSpec The field specification for the column being transformed and created in Pinot
   * @param columnReader The source column reader to read raw data from
   * @param additionalTransformers Additional custom transformers to apply after built-in transformers.
   */
  public ColumnReaderTransformer(TableConfig tableConfig, Schema schema,
      FieldSpec fieldSpec, ColumnReader columnReader, List<ColumnTransformer> additionalTransformers) {
    _columnReader = columnReader;
    _allTransformers = new ArrayList<>();
    _nullValueTransformer = new NullValueColumnTransformer(tableConfig, fieldSpec, schema);
    addIfNotNoOp(_allTransformers, new DataTypeColumnTransformer(tableConfig, fieldSpec, columnReader));
    addIfNotNoOp(_allTransformers, _nullValueTransformer);
    for (ColumnTransformer transformer : additionalTransformers) {
      addIfNotNoOp(_allTransformers, transformer);
    }

    _allTransformersExceptNullTransformer = new ArrayList<>();
    for (ColumnTransformer transformer : _allTransformers) {
      if (!(transformer instanceof NullValueColumnTransformer)) {
        _allTransformersExceptNullTransformer.add(transformer);
      }
    }
  }

  private static void addIfNotNoOp(List<ColumnTransformer> transformers, @Nullable ColumnTransformer transformer) {
    if (transformer != null && !transformer.isNoOp()) {
      transformers.add(transformer);
    }
  }

  private Object applyTransformers(Object value) {
    for (ColumnTransformer transformer : _allTransformers) {
      value = transformer.transform(value);
    }
    return value;
  }

  /**
   * This API helps the client to either call type specific APIs (nextInt, nextLong, etc.) or
   * call the generic next() API based on whether any transformation is required.
   */
  public boolean requiresTransformation() {
    return !_allTransformersExceptNullTransformer.isEmpty();
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
    // TODO - consider checking transformers for null response similar to logic isNull(docId)
    //  It requires peeking the next value without advancing the reader.
    //  Once peek is supported in ColumnReader, we can implement this correctly.
    return _columnReader.isNextNull();
  }

  @Override
  public void skipNext()
      throws IOException {
    _columnReader.skipNext();
  }

  @Override
  public boolean isSingleValue() {
    return _columnReader.isSingleValue();
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
    if (isNextNull()) {
      return (int) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.nextInt();
    }
  }

  @Override
  public long nextLong()
      throws IOException {
    if (isNextNull()) {
      return (long) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.nextLong();
    }
  }

  @Override
  public float nextFloat()
      throws IOException {
    if (isNextNull()) {
      return (float) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.nextFloat();
    }
  }

  @Override
  public double nextDouble()
      throws IOException {
    if (isNextNull()) {
      return (double) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.nextDouble();
    }
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
    if (isNextNull()) {
      return (int[]) _nullValueTransformer.transform(null);
    }
    int[] value = _columnReader.nextIntMV();
    if (value.length == 0) {
      return (int[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public long[] nextLongMV()
      throws IOException {
    if (isNextNull()) {
      return (long[]) _nullValueTransformer.transform(null);
    }
    long[] value = _columnReader.nextLongMV();
    if (value.length == 0) {
      return (long[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public float[] nextFloatMV()
      throws IOException {
    if (isNextNull()) {
      return (float[]) _nullValueTransformer.transform(null);
    }
    float[] value = _columnReader.nextFloatMV();
    if (value.length == 0) {
      return (float[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public double[] nextDoubleMV()
      throws IOException {
    if (isNextNull()) {
      return (double[]) _nullValueTransformer.transform(null);
    }
    double[] value = _columnReader.nextDoubleMV();
    if (value.length == 0) {
      return (double[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
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

  // Check if the value itself is null or if any of the transformers would return null for the value
  // Eg: DataTypeColumnTransformer may convert to null (transform exceptions during MV to SV, String to Int, etc.)
  // This case is important because NullValueColumnTransformer will transform null to default value
  // In those cases, we still want isNull to return true so that the null vector index can be built correctly
  @Override
  public boolean isNull(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return true;
    }
    // If there are no transformers, avoid casting to Object unnecessarily
    if (!_allTransformersExceptNullTransformer.isEmpty()) {
      Object value = _columnReader.getValue(docId);
      for (ColumnTransformer transformer : _allTransformersExceptNullTransformer) {
        value = transformer.transform(value);
        if (value == null) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public int getInt(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (int) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.getInt(docId);
    }
  }

  @Override
  public long getLong(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (long) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.getLong(docId);
    }
  }

  @Override
  public float getFloat(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (float) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.getFloat(docId);
    }
  }

  @Override
  public double getDouble(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (double) _nullValueTransformer.transform(null);
    } else {
      return _columnReader.getDouble(docId);
    }
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
  public Object getValue(int docId)
      throws IOException {
    return applyTransformers(_columnReader.getValue(docId));
  }

  @Override
  public int[] getIntMV(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (int[]) _nullValueTransformer.transform(null);
    }
    int[] value = _columnReader.getIntMV(docId);
    if (value.length == 0) {
      return (int[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public long[] getLongMV(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (long[]) _nullValueTransformer.transform(null);
    }
    long[] value = _columnReader.getLongMV(docId);
    if (value.length == 0) {
      return (long[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public float[] getFloatMV(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (float[]) _nullValueTransformer.transform(null);
    }
    float[] value = _columnReader.getFloatMV(docId);
    if (value.length == 0) {
      return (float[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
  }

  @Override
  public double[] getDoubleMV(int docId)
      throws IOException {
    if (_columnReader.isNull(docId)) {
      return (double[]) _nullValueTransformer.transform(null);
    }
    double[] value = _columnReader.getDoubleMV(docId);
    if (value.length == 0) {
      return (double[]) _nullValueTransformer.transform(null);
    } else {
      return value;
    }
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
