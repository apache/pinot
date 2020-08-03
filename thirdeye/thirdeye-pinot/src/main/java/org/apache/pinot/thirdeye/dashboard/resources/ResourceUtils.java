/*
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
 *
 */

package org.apache.pinot.thirdeye.dashboard.resources;

/**
 * This class is expected to house static utilities that help responding to resource requests.
 */
public class ResourceUtils {

  /**
   * ensure a condition is true and respond with bad request otherwise
   *
   * @param condition ensure this condition is true
   * @param message message sent in the 400 response to the user in plain text
   */
  public static void ensure(boolean condition, String message) {
    if (!condition) {
      throw new BadRequestWebException(message);
    }
  }

  /**
   * ensure the object exists and respond with bad request otherwise
   *
   * @param o ensure this object is not null
   * @param message message sent in the 400 response to the user in plain text
   * @return o (same object) if exists
   */
  public static <T> T ensureExists(T o, String message) {
    ensure(o != null, message);
    return o;
  }

  /**
   * Utility method to throw badRequest
   * @param errorMsg msg to be sent to user
   * @return instance of BadRequestWebException
   */
  public static BadRequestWebException badRequest(String errorMsg) {
    return new BadRequestWebException(errorMsg);
  }
}
