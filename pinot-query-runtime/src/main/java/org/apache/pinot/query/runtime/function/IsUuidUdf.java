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
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.scalar.uuid.IsUuidScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


@AutoService(Udf.class)
public class IsUuidUdf extends Udf {
  private static final IsUuidScalarFunction SCALAR_FUNCTION = new IsUuidScalarFunction();

  @Override
  public String getMainName() {
    return SCALAR_FUNCTION.getName();
  }

  @Override
  public Set<String> getAllNames() {
    return SCALAR_FUNCTION.getNames();
  }

  @Override
  public String getDescription() {
    return "Returns true when the input is a valid RFC 4122 UUID string or a 16-byte BYTES value.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return Collections.emptyMap();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
