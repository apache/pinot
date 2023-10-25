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
package org.apache.pinot.core.common.inverted;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.transform.function.JsonExtractScalarTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class InvertedDataFetcherFactory {

  private InvertedDataFetcherFactory() {
  }

  public static InvertedDataFetcher get(
      String column, DataSource dataSource, @Nullable TransformFunction transformFunction) {
    if (transformFunction == null) {
      return new InvertedIndexDataFetcher(column, dataSource);
    }
    if (transformFunction instanceof JsonExtractScalarTransformFunction) {
      JsonExtractScalarTransformFunction jsonExtractScalarTransformFunction
          = (JsonExtractScalarTransformFunction) transformFunction;
      Preconditions.checkState(jsonExtractScalarTransformFunction.getResultMetadata().isSingleValue(),
          "Only SV results supported for json_extract_scalar with inverted group-by");
      String jsonPathString = jsonExtractScalarTransformFunction.getJsonPathString();
      return new InvertedJsonIndexDataFetcher(dataSource, jsonPathString);
    }
    throw new UnsupportedOperationException("Unable to run Inverted Group-By");
  }
}
