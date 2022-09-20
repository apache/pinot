package org.apache.pinot.spi.utils;

public class CommonUtils {
  /**
   * Rethrows an exception, even if it is not in the method signature.
   *
   * @param t The exception to rethrow.
   */
  public static void rethrowException(Throwable t) {
    /* Error can be thrown anywhere and is type erased on rethrowExceptionInner, making the cast in
    rethrowExceptionInner a no-op, allowing us to rethrow the exception without declaring it. */
    CommonUtils.<Error>rethrowExceptionInner(t);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrowExceptionInner(Throwable exception)
      throws T {
    throw (T) exception;
  }

}
