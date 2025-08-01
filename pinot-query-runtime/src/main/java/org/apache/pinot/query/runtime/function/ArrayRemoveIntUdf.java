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
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArrayRemoveIntUdf extends Udf.FromAnnotatedMethod {
  public ArrayRemoveIntUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayRemoveInt", int[].class, int.class));
  }

  @Override
  public String getDescription() {
    return "Removes _the first occurrence_ of the specified integer from the input array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Input array of integers"),
            UdfParameter.of("value", FieldSpec.DataType.INT)
                .withDescription("Integer value to remove from array"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Array after removing the specified value")
        ))
        .addExample("remove present value", List.of(1, 2, 3, 2), 2, List.of(1, 3, 2))
        .addExample("remove absent value", List.of(1, 2, 3), 4, List.of(1, 2, 3))
        .addExample("remove from empty array", List.of(), 1, List.of())
        .addExample(UdfExample.create("null array", null, 2, null).withoutNull(List.of()))
        .addExample(UdfExample.create("null value", List.of(1, 2, 3), null, null)
            .withoutNull(List.of(1, 2, 3)))
        .addExample(UdfExample.create("null default value", List.of(0, 1, 2, 3), null, List.of())
            .withoutNull(List.of(1, 2, 3)))
        .build()
        .generateExamples();
  }
}
