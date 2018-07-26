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
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The <code>LiteralTransformFunction</code> class is a special transform function which is a wrapper on top of a
 * LITERAL, and only supports {@link #getLiteral()}.
 */
public class LiteralTransformFunction implements TransformFunction {
  private final String _literal;

  public LiteralTransformFunction(@Nonnull String literal) {
    _literal = literal;
  }

  public String getLiteral() {
    return _literal;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToDictIdsSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[][] transformToLongValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[][] transformToFloatValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[][] transformToDoubleValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(@Nonnull ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException();
  }
}
