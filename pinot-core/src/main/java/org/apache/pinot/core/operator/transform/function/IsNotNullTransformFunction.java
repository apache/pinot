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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


public class IsNotNullTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;

  @Override
  public String getName() {
    return TransformFunctionType.IS_NOT_NULL.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 1, "Exact 1 argument is required for IS_NULL operator function");
    _transformFunction = arguments.get(0);
    super.init(arguments, dataSourceMap);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    throw new UnsupportedOperationException("IsNotNull is not supported when enableNullHandling is set to false");
  }

  @Override
  public Pair<RoaringBitmap, int[]> transformToIntValuesSVWithNull(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[numDocs];
    }
    RoaringBitmap bitmap = _transformFunction.getNullBitmap(projectionBlock);
    if (bitmap == null) {
      Arrays.fill(_intValuesSV, 1);
      return ImmutablePair.of(null, _intValuesSV);
    }
    bitmap.forEach((IntConsumer) i -> _intValuesSV[i] = 1);
    bitmap.flip(0L, numDocs);
    return ImmutablePair.of(null , _intValuesSV);
  }

  @Override
  public RoaringBitmap getNullBitmap(ProjectionBlock projectionBlock) {
    return null;
  }
}
