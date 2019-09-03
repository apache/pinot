package org.apache.pinot.thirdeye.detection;

/**
 * The exception to be thrown in detection pipelines.
 */
public class DetectionPipelineException extends Exception {
  public DetectionPipelineException(Throwable cause) {
    super(cause);
  }

  public DetectionPipelineException() {
    super();
  }

  public DetectionPipelineException(String message) {
    super(message);
  }

  public DetectionPipelineException(String message, Throwable cause) {
    super(message, cause);
  }
}
