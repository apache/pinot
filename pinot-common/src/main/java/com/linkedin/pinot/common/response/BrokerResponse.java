/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.response;

import java.util.List;
import org.json.JSONObject;


/**
 * Interface for broker response.
 */
public interface BrokerResponse {

  /**
   * Set exceptions caught during request handling, into the broker response.
   * @param exceptions
   */
  void setExceptions(List<ProcessingException> exceptions);

  /**
   * Set the total time used in request handling, into the broker response.
   * @param timeUsedMs
   */
  void setTimeUsedMs(long timeUsedMs);

  /**
   * Convert the broker response to JSONObject.
   * @return
   * @throws Exception
   */
  JSONObject toJson()
      throws Exception;

  /**
   * Convert the broker response to JSON String.
   * @return
   * @throws Exception
   */
  String toJsonString()
      throws Exception;

  /**
   * Return the responseType to indicate the implementation class.
   * @return
   */
  BrokerResponseFactory.ResponseType getResponseType();

  /**
   * Return the number of documents scanned when processing the query.
   * @return
   */
  long getNumDocsScanned();

  /**
   * Return the number of exceptions recorded in the response.
   * @return
   */
  public int getExceptionsSize();
}
