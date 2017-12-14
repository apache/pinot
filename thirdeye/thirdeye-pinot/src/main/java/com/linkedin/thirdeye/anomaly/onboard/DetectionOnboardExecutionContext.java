package com.linkedin.thirdeye.anomaly.onboard;

import java.util.HashMap;
import java.util.Map;

/**
 * A container to store the execution context (i.e., the input for a task or the output from a task) of a job.
 * Currently, we assume that the order of tasks forms a total order and therefore there is no concurrent tasks.
 */
public class DetectionOnboardExecutionContext {
  private Map<String, Object> executionResults = new HashMap<>();

  /**
   * Sets the execution context (i.e., result or input) with the given key.
   *
   * @param key the key to associate with the given execution context (value).
   * @param value the execution context.
   *
   * @return the previous value associated with the key, or null if there was no mapping for the key.
   */
  public Object setExecutionResult(String key, Object value) {
    return this.executionResults.put(key, value);
  }

  /**
   * Returns the execution context to which the specified key is associated, or null if no such mapping for the key.
   *
   * @param key key with which the specified execution context is to be associated.
   *
   * @return the execution context to which the specified key is associated, or null if no such mapping for the key.
   */
  public Object getExecutionResult(String key) {
    return this.executionResults.get(key);
  }
}
