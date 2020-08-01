package org.apache.pinot.thirdeye.dashboard.resources;

public class ResourceUtils {

  public static void ensure(boolean condition, String message) {
    if (!condition) {
      throw new BadRequestWebException(message);
    }
  }

  public static <T> T ensureExists(T o, String message) {
    ensure(o != null, message);
    return o;
  }

  public static BadRequestWebException badRequest(String errorMsg) {
    return new BadRequestWebException(errorMsg);
  }
}
