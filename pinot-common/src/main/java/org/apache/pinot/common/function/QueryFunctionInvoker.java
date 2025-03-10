package org.apache.pinot.common.function;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Like [FunctionInvoker], but designed to be called from query engines.
///
/// The main difference is that methods on this class throws
/// [QueryException][org.apache.pinot.spi.exception.QueryException]s instead of generic [Exception]s.
public class QueryFunctionInvoker {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryFunctionInvoker.class);

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
      String msg = "Caught exception while invoking method: " + getMethod()
          + " with arguments: " + Arrays.toString(arguments);
      LOGGER.info(msg, e);
      throw QueryErrorCode.QUERY_EXECUTION.asException(msg + ": " + e.getMessage());
    } catch (IllegalAccessException e) {
      String msg = "Caught exception while invoking method: " + getMethod() + " with arguments: "
          + Arrays.toString(arguments);
      LOGGER.warn(msg, e);
      throw QueryErrorCode.INTERNAL.asException(msg);
    }
  }
}
