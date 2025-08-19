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
package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.arithmetic.PlusScalarFunction;
import org.apache.pinot.core.operator.transform.function.AdditionTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfSignature;


@AutoService(Udf.class)
public class PlusUdf extends Udf {
  @Override
  public String getMainName() {
    return "plus";
  }

  @Override
  public Set<String> getAllNames() {
    return Set.of(getMainName(), "add");
  }

  @Override
  public String getDescription() {
    return "This function adds two numeric values together. In order to concatenate two strings, use the `concat` "
        + "function instead.";
  }

  @Override
  public String asSqlCall(String name, List<String> sqlArgValues) {
    if (name.equals(getMainCanonicalName())) {
      return "(" + String.join(" + ", sqlArgValues) + ")";
    } else {
      return super.asSqlCall(name, sqlArgValues);
    }
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forEndomorphismNumeric(2)
        .addExample("1 + 2", 1, 2, 3)
        .addExample(UdfExample.create("1 + null", 1, null, null).withoutNull(1))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.ADD, AdditionTransformFunction.class);
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return new PlusScalarFunction();
  }
}
