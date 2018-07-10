/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The <code>IdentifierTransformFunction</code> class is a special transform function which is a wrapper on top of an
 * IDENTIFIER (column), and directly return the column value without any transformation.
 */
public class IdentifierTransformFunction implements TransformFunction {
  private final String _columnName;
  private final DataSource _dataSource;
  private final TransformResultMetadata _resultMetadata;

  public IdentifierTransformFunction(@Nonnull String columnName, @Nonnull DataSource dataSource) {
    _columnName = columnName;
    _dataSource = dataSource;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    _resultMetadata = new TransformResultMetadata(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
        dataSourceMetadata.hasDictionary());
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void init(@Nonnull List<TransformFunction> arguments, @Nonnull Map<String, DataSource> dataSourceMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public Dictionary getDictionary() {
    return _dataSource.getDictionary();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsSV();
  }

  @Override
  public int[][] transformToDictIdsMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDictionaryIdsMV();
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesSV();
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesSV();
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesSV();
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesSV();
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesSV();
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getIntValuesMV();
  }

  @Override
  public long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getLongValuesMV();
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getFloatValuesMV();
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getDoubleValuesMV();
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    return projectionBlock.getBlockValueSet(_columnName).getStringValuesMV();
  }
}
