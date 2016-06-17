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
package com.linkedin.pinot.common.restlet.swagger;

/**
 * A response for an API call.
 */
public @interface Response {
  /**
   * The HTTP Status Code that can be returned by this API call or the String "default" for the default response object.
   *
   * @return A numeric HTTP status code or the string "default"
   */
  String statusCode();

  /**
   * A description of the response.
   *
   * @return A textual description of the response.
   */
  String description();
}
