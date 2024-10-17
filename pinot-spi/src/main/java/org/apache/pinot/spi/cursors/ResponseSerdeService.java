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
package org.apache.pinot.spi.cursors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
public class ResponseSerdeService {
  private static volatile ResponseSerdeService _instance = fromServiceLoader();

  private final Set<ResponseSerde> _allResponseSerde;
  private final Map<String, ResponseSerde> _responseSerdeByType;

  private ResponseSerdeService(Set<ResponseSerde> serdeSet) {
    _allResponseSerde = serdeSet;
    _responseSerdeByType = new HashMap<>();

    for (ResponseSerde responseSerde : serdeSet) {
      _responseSerdeByType.put(responseSerde.getType(), responseSerde);
    }
  }

  public static ResponseSerdeService getInstance() {
    return _instance;
  }

  public static void setInstance(ResponseSerdeService service) {
    _instance = service;
  }

  public static ResponseSerdeService fromServiceLoader() {
    Set<ResponseSerde> serdeSet = new HashSet<>();
    for (ResponseSerde responseSerde : ServiceLoader.load(ResponseSerde.class)) {
      serdeSet.add(responseSerde);
    }

    return new ResponseSerdeService(serdeSet);
  }

  public Set<ResponseSerde> getAllResponseSerde() {
    return _allResponseSerde;
  }

  public Map<String, ResponseSerde> getResponseSerdeByType() {
    return _responseSerdeByType;
  }

  public ResponseSerde getResponseSerde(String type) {
    ResponseSerde responseSerde = _responseSerdeByType.get(type);

    if (responseSerde == null) {
      throw new IllegalArgumentException("Unknown ResponseSerde type: " + type);
    }

    return responseSerde;
  }
}
