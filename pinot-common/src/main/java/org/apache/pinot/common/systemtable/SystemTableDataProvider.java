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
package org.apache.pinot.common.systemtable;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.systemtable.SystemTableProvider;


/**
 * Extension of {@link SystemTableProvider} that can supply data for a system table query using the standard query
 * request/response types.
 */
public interface SystemTableDataProvider extends SystemTableProvider {

  /**
   * Fetch rows for a system table query using the parsed {@link PinotQuery}. Implementations should respect the
   * projection/filter/offset/limit encoded in the query and return a fully-formed {@link BrokerResponseNative}.
   */
  BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery)
      throws Exception;
}
