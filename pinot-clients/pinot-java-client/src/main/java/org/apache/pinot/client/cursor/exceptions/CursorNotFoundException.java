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
package org.apache.pinot.client.cursor.exceptions;

/**
 * Exception thrown when attempting to use a cursor that does not exist.
 * This can happen if the cursor ID is invalid or if the cursor has been deleted.
 */
public class CursorNotFoundException extends CursorException {

  private final String _cursorId;

  /**
   * Creates a new CursorNotFoundException.
   *
   * @param cursorId the ID of the cursor that was not found
   */
  public CursorNotFoundException(String cursorId) {
    super(String.format("Cursor '%s' not found", cursorId));
    _cursorId = cursorId;
  }

  /**
   * Creates a new CursorNotFoundException with a custom message.
   *
   * @param cursorId the ID of the cursor that was not found
   * @param message custom error message
   */
  public CursorNotFoundException(String cursorId, String message) {
    super(message);
    _cursorId = cursorId;
  }

  /**
   * Creates a new CursorNotFoundException with a custom message and cause.
   *
   * @param cursorId the ID of the cursor that was not found
   * @param message custom error message
   * @param cause the cause of this exception
   */
  public CursorNotFoundException(String cursorId, String message, Throwable cause) {
    super(message, cause);
    _cursorId = cursorId;
  }

  /**
   * Gets the ID of the cursor that was not found.
   *
   * @return cursor ID
   */
  public String getCursorId() {
    return _cursorId;
  }
}
