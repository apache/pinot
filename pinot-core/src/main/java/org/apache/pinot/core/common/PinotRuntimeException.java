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
package org.apache.pinot.core.common;

/**
 * The base runtime exception for Pinot.
 *
 * Notice that this class was introduced in the Pinot 1.4.0 release and the vast majority of the codebase still uses
 * {@link RuntimeException} directly. We should gradually migrate to this class.
 *
 */
public class PinotRuntimeException extends RuntimeException {
  public PinotRuntimeException() {
  }

  public PinotRuntimeException(String message) {
    super(message);
  }

  public PinotRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public PinotRuntimeException(Throwable cause) {
    super(cause);
  }
}
