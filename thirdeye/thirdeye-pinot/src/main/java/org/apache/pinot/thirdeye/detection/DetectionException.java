package org.apache.pinot.thirdeye.detection;

/**
 * The exception to be thrown in detection pipelines.
 */
public class DetectionException extends Exception {
  public DetectionException(Throwable cause) {
    super(cause);
  }

  public DetectionException() {
    super();
  }

  public DetectionException(String message) {
    super(message);
  }

  public DetectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
