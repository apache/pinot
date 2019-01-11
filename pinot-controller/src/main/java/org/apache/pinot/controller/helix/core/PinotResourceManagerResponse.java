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
package org.apache.pinot.controller.helix.core;

public class PinotResourceManagerResponse {
  public static final PinotResourceManagerResponse SUCCESS = new PinotResourceManagerResponse(true, null);
  public static final PinotResourceManagerResponse FAILURE = new PinotResourceManagerResponse(false, null);

  private final boolean _successful;
  private final String _message;

  public static PinotResourceManagerResponse success(String message) {
    return new PinotResourceManagerResponse(true, message);
  }

  public static PinotResourceManagerResponse failure(String message) {
    return new PinotResourceManagerResponse(false, message);
  }

  private PinotResourceManagerResponse(boolean successful, String message) {
    _successful = successful;
    _message = message;
  }

  public boolean isSuccessful() {
    return _successful;
  }

  public String getMessage() {
    return _message;
  }

  @Override
  public String toString() {
    if (_successful) {
      return _message == null ? "SUCCESS" : "SUCCESS: " + _message;
    } else {
      return _message == null ? "FAILURE" : "FAILURE: " + _message;
    }
  }
}
