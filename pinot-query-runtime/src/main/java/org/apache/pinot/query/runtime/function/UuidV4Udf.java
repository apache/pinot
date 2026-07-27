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
import org.apache.pinot.common.function.scalar.uuid.UuidConversionFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


@AutoService(Udf.class)
public class UuidV4Udf extends Udf.FromAnnotatedMethod {
  public UuidV4Udf()
      throws NoSuchMethodException {
    super(UuidConversionFunctions.class.getMethod("uuidV4"));
  }

  @Override
  public String getDescription() {
    return "Generates a fresh random RFC 4122 version-4 UUID. Each invocation produces a new value.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return Map.of();
  }
}
