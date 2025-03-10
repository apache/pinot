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
