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
package org.apache.pinot.common.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.exception.QueryErrorCode;

/// Like [FunctionInvoker], but designed to be called from query engines.
///
/// The main difference is that methods on this class throws
/// [QueryException][org.apache.pinot.spi.exception.QueryException]s instead of generic [Exception]s.
public class QueryFunctionInvoker {

  private final FunctionInvoker _functionInvoker;

  public QueryFunctionInvoker(FunctionInfo functionInfo) {
    _functionInvoker = new FunctionInvoker(functionInfo);
  }

  public Method getMethod() {
    return _functionInvoker.getMethod();
  }

  public Class<?>[] getParameterClasses() {
    return _functionInvoker.getParameterClasses();
  }

  public PinotDataType[] getParameterTypes() {
    return _functionInvoker.getParameterTypes();
  }

  public void convertTypes(Object[] arguments) {
    try {
      _functionInvoker.convertTypes(arguments);
    } catch (Exception e) {
      String errorMsg = "Invalid conversion when calling " + getMethod() + ": " + e.getMessage();
      throw QueryErrorCode.QUERY_EXECUTION.asException(errorMsg, e);
    }
  }

  public Class<?> getResultClass() {
    return _functionInvoker.getResultClass();
  }

  @Nullable
  public Object invoke(Object[] arguments) {
    try {
      return _functionInvoker.invokeDirectly(arguments);
    } catch (InvocationTargetException e) {
      throw QueryErrorCode.QUERY_EXECUTION.asException(e);
    } catch (IllegalAccessException e) {
      throw QueryErrorCode.INTERNAL.asException(e);
    }
  }
}
