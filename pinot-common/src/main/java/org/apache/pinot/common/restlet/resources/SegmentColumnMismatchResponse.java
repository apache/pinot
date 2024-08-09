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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This class gives the data of a server if there exists any segments that need to be reloaded
 *
 * It has details of server id and returns true/false if there are any segments to be reloaded or not.
 */
public class SegmentColumnMismatchResponse {
  boolean _isMismatch;
  String _serverInstanceId;

  @JsonCreator
  public SegmentColumnMismatchResponse(@JsonProperty("isMismatch") boolean isMismatch,
      @JsonProperty("serverInstanceId") String serverInstanceId) {
    _isMismatch = isMismatch;
    _serverInstanceId = serverInstanceId;
  }

  public String getServerInstanceId() {
    return _serverInstanceId;
  }

  public boolean getMismatch() {
    return _isMismatch;
  }
}
