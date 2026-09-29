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
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.core.operator.transform.function.DateTimeTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class YearUdf extends Udf.FromAnnotatedMethod {

  public YearUdf()
      throws NoSuchMethodException {
    super(DateTimeFunctions.class.getMethod("year", long.class));
  }

  @Override
  public String getDescription() {
    return "Returns the year from the given epoch millis in UTC timezone.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(
            UdfSignature.of(
                UdfParameter.of("millis", FieldSpec.DataType.LONG)
                    .withDescription("A long value representing epoch millis"
                        + "e.g., 1577836800000L for 2020-01-01T00:00:00Z"),
                UdfParameter.result(FieldSpec.DataType.INT)
                    .withDescription("Returns the year as an integer")
            ))
        .addExample("2020-01-01T00:00:00Z", 1577836800000L, 2020)
        .addExample("1970-01-01T00:00:00Z", 0L, 1970)
        .addExample(UdfExample.create("null input", null, null).withoutNull(1970))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.YEAR, DateTimeTransformFunction.Year.class);
  }
}
