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
package org.apache.pinot.sql.parsers;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.parsers.QueryCompiler;
import org.apache.pinot.pql.parsers.PinotQuery2BrokerRequestConverter;


/**
 * CalciteSqlCompiler is a Calcite SQL compiler.
 */
public class CalciteSqlCompiler implements QueryCompiler {

  @Override
  public BrokerRequest compileToBrokerRequest(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    return converter.convert(pinotQuery);
  }
}
