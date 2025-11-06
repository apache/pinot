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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


/**
 * UDF for array function.
 */
@AutoService(Udf.class)
public class ArrayUdf extends Udf.FromAnnotatedMethod {
  public ArrayUdf()
      throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayValueConstructor", Object[].class));
  }

  @Override
  public String getDescription() {
    return "Constructs an array from the given arguments. Usage: array(element1, element2, ...).";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    // TODO: Support variadic parameters in UdfExampleBuilder and scenarios
    return Collections.emptyMap();
  }
}
