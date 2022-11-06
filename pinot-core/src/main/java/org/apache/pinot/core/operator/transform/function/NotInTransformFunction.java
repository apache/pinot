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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The NOT_IN transform function is the negate version of the {@link InTransformFunction}.
 */
public class NotInTransformFunction extends InTransformFunction {

  @Override
  public String getName() {
    return TransformFunctionType.NOT_IN.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    super.init(arguments, dataSourceMap);
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int[] intValuesSV = super.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < intValuesSV.length; i++) {
      intValuesSV[i] = 1 - intValuesSV[i];
    }
    return intValuesSV;
  }
}
