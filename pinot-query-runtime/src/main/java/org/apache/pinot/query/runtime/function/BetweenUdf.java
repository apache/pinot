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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.scalar.comparison.BetweenScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class BetweenUdf extends Udf {
  @Override
  public String getMainName() {
    return "between";
  }

  @Override
  public String getDescription() {
    return "Returns true if the first argument is between the second and third arguments (inclusive), "
        + "false otherwise. "
        + "It supports numerics and strings as arguments. ";
  }

  @Override
  public String asSqlCall(String name, List<String> sqlArgValues) {
    return sqlArgValues.get(0) + " BETWEEN " + sqlArgValues.get(1) + " AND " + sqlArgValues.get(2);
  }

  @Nullable
  @Override
  public PinotScalarFunction getScalarFunction() {
    return new BetweenScalarFunction();
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("value", FieldSpec.DataType.INT),
            UdfParameter.of("lower", FieldSpec.DataType.INT),
            UdfParameter.of("upper", FieldSpec.DataType.INT),
            UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
        .addExample("inside range", 1, 0, 3, true)
        .addExample("outside range", 4, 0, 3, false)
        .addExample("null value", null, 0, 3, null)
        .addExample("null lower", 0, null, 3, null)
        .addExample("null upper", 0, -1, null, null)
        .and(UdfExampleBuilder.forSignature(UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.LONG),
                UdfParameter.of("lower", FieldSpec.DataType.LONG),
                UdfParameter.of("upper", FieldSpec.DataType.LONG),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
            .addExample("inside range", 1L, 0L, 3L, true)
            .addExample("outside range", 4L, 0L, 3L, false)
            .addExample("null value", null, 0L, 3L, null)
            .addExample("null lower", 0L, null, 3L, null)
            .addExample("null upper", 0L, -1L, null, null)
            .build())
        .and(UdfExampleBuilder.forSignature(UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.FLOAT),
                UdfParameter.of("lower", FieldSpec.DataType.FLOAT),
                UdfParameter.of("upper", FieldSpec.DataType.FLOAT),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
            .addExample("inside range", 1f, 0f, 3f, true)
            .addExample("outside range", 4f, 0f, 3f, false)
            .addExample("null value", null, 0f, 3f, null)
            .addExample("null lower", 0f, null, 3f, null)
            .addExample("null upper", 0f, -1f, null, null)
            .build())
        .and(UdfExampleBuilder.forSignature(UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.DOUBLE),
                UdfParameter.of("lower", FieldSpec.DataType.DOUBLE),
                UdfParameter.of("upper", FieldSpec.DataType.DOUBLE),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
            .addExample("inside range", 1d, 0d, 3d, true)
            .addExample("outside range", 4d, 0d, 3d, false)
            .addExample("null value", null, 0d, 3d, null)
            .addExample("null lower", 0d, null, 3d, null)
            .addExample("null upper", 0d, -1d, null, null)
            .build())
        .and(UdfExampleBuilder.forSignature(UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.BIG_DECIMAL),
                UdfParameter.of("lower", FieldSpec.DataType.BIG_DECIMAL),
                UdfParameter.of("upper", FieldSpec.DataType.BIG_DECIMAL),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
            .addExample("inside range", new BigDecimal(1), new BigDecimal(1), new BigDecimal(1), true)
            .addExample("outside range", new BigDecimal(4), new BigDecimal(1), new BigDecimal(1), false)
            .addExample("null value", null, new BigDecimal(1), new BigDecimal(1), null)
            .addExample("null lower", new BigDecimal(1), null, new BigDecimal(1), null)
            .addExample("null upper", new BigDecimal(1), new BigDecimal(-1), null, null)
            .build())
        .and(UdfExampleBuilder.forSignature(UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.STRING),
                UdfParameter.of("lower", FieldSpec.DataType.STRING),
                UdfParameter.of("upper", FieldSpec.DataType.STRING),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)))
            .addExample("inside range", "b", "a", "c", true)
            .addExample("outside range", "e", "a", "d", false)
            .addExample("null value", null, "", "d", null)
            .addExample("null lower", "", null, "a", null)
            .addExample("null upper", "", "", null, null)
            .build())
        .generateExamples();
  }
}
