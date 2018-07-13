/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple code to invoke a method in any class using reflection.
 * Eventually this will support annotations on the method but for now its a simple wrapper on any java method
 */
public class FunctionInvoker {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionInvoker.class);
  // Don't log more than 10 entries in 5 MINUTES 
  //TODO:Convert this functionality into a class that can be used in other places
  private static long EXCEPTION_LIMIT_DURATION = TimeUnit.MINUTES.toMillis(5);
  private static long EXCEPTION_LIMIT_RATE = 10;
  private Method _method;
  private Object _instance;
  private int exceptionCount;
  private long lastExceptionTime = 0;
  private FunctionInfo _functionInfo;

  public FunctionInvoker(FunctionInfo functionInfo) throws Exception {
    _functionInfo = functionInfo;
    _method = functionInfo.getMethod();
    Class<?> clazz = functionInfo.getClazz();
    if (Modifier.isStatic(_method.getModifiers())) {
      _instance = null;
    } else {
      _instance = clazz.newInstance();
    }
  }

  public Class<?>[] getParameterTypes() {
    return _method.getParameterTypes();
  }

  public Class<?> getReturnType() {
    return _method.getReturnType();
  }

  public Object process(Object[] args) {
    try {
      return _method.invoke(_instance, _functionInfo.convertTypes(args));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      //most likely the exception is in the udf,  get the exceptio
      Throwable cause = e.getCause();
      if (cause == null) {
        cause = e;
      }
      //some udf's might be configured incorrectly and we dont want to pollute the log
      //keep track of the last time an exception was logged and reset the counter if the last exception is more than the EXCEPTION_LIMIT_DURATION
      if(Duration.millis(System.currentTimeMillis() - lastExceptionTime).getStandardMinutes() > EXCEPTION_LIMIT_DURATION) {
        exceptionCount = 0;
      }
      if(exceptionCount < EXCEPTION_LIMIT_RATE) {
        exceptionCount = exceptionCount + 1;
        LOGGER.error("Exception invoking method:{} with args:{}, exception message: {}", _method.getName(), Arrays.toString(args), cause.getMessage());
      }
      return null;
    }
  }

}
