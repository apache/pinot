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
package org.apache.pinot.core.udf;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/// The signature of a User Defined Function ([UDF][Udf]) used to define examples.
///
/// This is a simplified version of the signature defined by Calcite, which is quite more expressive (ie the result
/// type can depend on the parameters).
public abstract class UdfSignature {
  public abstract List<UdfParameter> getParameters();

  public abstract UdfParameter getReturnType();

  public int getArity() {
    return getParameters().size();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof UdfSignature)) {
      return false;
    }
    UdfSignature other = (UdfSignature) o;
    return getReturnType().equals(other.getReturnType()) && getParameters().equals(other.getParameters());
  }

  @Override
  public int hashCode() {
    return 31 * getParameters().hashCode() + getReturnType().hashCode();
  }

  @Override
  public String toString() {
    return getParameters().stream()
        .map(UdfParameter::toString)
        .collect(Collectors.joining(", ", "(", ")")) + " -> " + getReturnType().getTypeString();
  }

  public static UdfSignature of(UdfParameter... parametersAndReturnType) {
    return new UdfSignature() {
      @Override
      public List<UdfParameter> getParameters() {
        return Arrays.asList(parametersAndReturnType).subList(0, parametersAndReturnType.length - 1);
      }

      @Override
      public UdfParameter getReturnType() {
        return parametersAndReturnType[parametersAndReturnType.length - 1];
      }
    };
  }
}
