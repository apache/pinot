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
 * Exception thrown when attempting to use an expired cursor.
 * Cursors have a limited lifetime and will expire after a certain period of inactivity.
 */
public class CursorExpiredException extends CursorException {

  private final String _cursorId;
  private final long _expirationTime;

  /**
   * Creates a new CursorExpiredException.
   *
   * @param cursorId the ID of the expired cursor
   * @param expirationTime the expiration timestamp
   */
  public CursorExpiredException(String cursorId, long expirationTime) {
    super(String.format("Cursor '%s' has expired at %d", cursorId, expirationTime));
    _cursorId = cursorId;
    _expirationTime = expirationTime;
  }

  /**
   * Creates a new CursorExpiredException with a custom message.
   *
   * @param cursorId the ID of the expired cursor
   * @param expirationTime the expiration timestamp
   * @param message custom error message
   */
  public CursorExpiredException(String cursorId, long expirationTime, String message) {
    super(message);
    _cursorId = cursorId;
    _expirationTime = expirationTime;
  }

  /**
   * Gets the ID of the expired cursor.
   *
   * @return cursor ID
   */
  public String getCursorId() {
    return _cursorId;
  }

  /**
   * Gets the expiration timestamp.
   *
   * @return expiration time in milliseconds
   */
  public long getExpirationTime() {
    return _expirationTime;
  }
}
