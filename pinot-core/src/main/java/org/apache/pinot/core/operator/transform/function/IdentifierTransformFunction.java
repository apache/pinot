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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.evaluator.TransformEvaluator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


/**
 * The <code>IdentifierTransformFunction</code> class is a special transform function which is a wrapper on top of an
 * IDENTIFIER (column), and directly return the column value without any transformation.
 */
public class IdentifierTransformFunction implements TransformFunction, PushDownTransformFunction {
  private final String _columnName;
  private final Dictionary _dictionary;
  private final TransformResultMetadata _resultMetadata;

  public IdentifierTransformFunction(String columnName, DataSource dataSource) {
    _columnName = columnName;
    _dictionary = dataSource.getDictionary();
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    _resultMetadata = new TransformResultMetadata(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
        _dictionary != null);
  }

  public String getColumnName() {
    return _columnName;
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public int[] transformToDictIdsSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsSV();
  }

  @Override
  public int[][] transformToDictIdsMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsMV();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesSV();
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesSV();
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesSV();
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesSV();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getBlockValueSet(_columnName).getNumSVEntries();
    BigDecimal[] bigDecimalValues = new BigDecimal[length];
    switch (_resultMetadata.getDataType()) {
      case INT:
      case BOOLEAN:
        int[] intValues = projectionBlock.getBlockValueSet(_columnName).getIntValuesSV();
        ArrayCopyUtils.copy(intValues, bigDecimalValues, length);
        break;
      case LONG:
      case TIMESTAMP:
        long[] longValues = projectionBlock.getBlockValueSet(_columnName).getLongValuesSV();
        ArrayCopyUtils.copy(longValues, bigDecimalValues, length);
        break;
      case FLOAT:
        float[] floatValues = projectionBlock.getBlockValueSet(_columnName).getFloatValuesSV();
        ArrayCopyUtils.copy(floatValues, bigDecimalValues, length);
        break;
      case DOUBLE:
        double[] doubleValues = projectionBlock.getBlockValueSet(_columnName).getDoubleValuesSV();
        ArrayCopyUtils.copy(doubleValues, bigDecimalValues, length);
        break;
      case BIG_DECIMAL:
      case BYTES:
        byte[][] byteValues = projectionBlock.getBlockValueSet(_columnName).getBytesValuesSV();
        ArrayCopyUtils.copy(byteValues, bigDecimalValues, length);
        break;
      case STRING:
      case JSON:
        String[] stringValues = projectionBlock.getBlockValueSet(_columnName).getStringValuesSV();
        ArrayCopyUtils.copy(stringValues, bigDecimalValues, length);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    return bigDecimalValues;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesSV();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getBytesValuesSV();
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesMV();
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesMV();
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesMV();
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesMV();
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesMV();
  }

  @Override
  public void transformToIntValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, int[] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToLongValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, long[] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToFloatValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, float[] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToDoubleValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      double[] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToBigDecimalValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      BigDecimal[] buffer) {
    int length = buffer.length;
    switch (_resultMetadata.getDataType()) {
      case INT:
      case BOOLEAN:
        int[] intBuffer = new int[length];
        projectionBlock.fillValues(_columnName, evaluator, intBuffer);
        ArrayCopyUtils.copy(intBuffer, buffer, length);
        break;
      case LONG:
      case TIMESTAMP:
        long[] longBuffer = new long[length];
        projectionBlock.fillValues(_columnName, evaluator, longBuffer);
        ArrayCopyUtils.copy(longBuffer, buffer, length);
        break;
      case FLOAT:
        float[] floatBuffer = new float[length];
        projectionBlock.fillValues(_columnName, evaluator, floatBuffer);
        ArrayCopyUtils.copy(floatBuffer, buffer, length);
        break;
      case DOUBLE:
        double[] doubleBuffer = new double[length];
        projectionBlock.fillValues(_columnName, evaluator, doubleBuffer);
        ArrayCopyUtils.copy(doubleBuffer, buffer, length);
        break;
      case STRING:
      case JSON:
        String[] stringBuffer = new String[length];
        projectionBlock.fillValues(_columnName, evaluator, stringBuffer, true);
        ArrayCopyUtils.copy(stringBuffer, buffer, length);
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void transformToStringValuesSV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      String[] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer, false);
  }

  @Override
  public void transformToIntValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, int[][] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToLongValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator, long[][] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToFloatValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      float[][] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToDoubleValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      double[][] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }

  @Override
  public void transformToStringValuesMV(ProjectionBlock projectionBlock, TransformEvaluator evaluator,
      String[][] buffer) {
    projectionBlock.fillValues(_columnName, evaluator, buffer);
  }
}
