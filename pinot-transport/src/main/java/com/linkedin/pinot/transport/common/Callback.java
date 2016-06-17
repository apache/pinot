/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.common;

/**
 * Common callback interface for different asynchronous operations
 *
 * @param <T>
 */
public interface Callback<T> {

  /**
   * Callback to indicate successful completion of an operation
   * @param arg0 Result of operation
   */
  public void onSuccess(T arg0);

  /**
   * Callback to indicate error
   * @param arg0 Throwable
   */
  public void onError(Throwable arg0);
}
