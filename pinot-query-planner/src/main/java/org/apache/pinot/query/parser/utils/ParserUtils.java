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
package org.apache.pinot.query.parser.utils;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParserUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParserUtils.class);

  private ParserUtils() {
  }

  /**
   * @param query the query string to be parsed and compiled
   * @param calciteSchema the Calcite schema to be used for compilation
   * @return true if the query can be parsed and compiled using the v2 multi-stage query engine
   */
  public static boolean canCompileQueryUsingV2Engine(String query, CalciteSchema calciteSchema) {
    // try to parse and compile the query with the Calcite planner used by the multi-stage query engine
    try {
      LOGGER.info("Trying to compile query `{}` using the multi-stage query engine", query);
      QueryEnvironment queryEnvironment =
          new QueryEnvironment(new TypeFactory(new TypeSystem()), calciteSchema, null, null);
      queryEnvironment.getTableNamesForQuery(query);
      LOGGER.info("Successfully compiled query using the multi-stage query engine: `{}`", query);
      return true;
    } catch (Exception e) {
      LOGGER.error("Encountered an error while compiling query `{}` using the multi-stage query engine", query, e);
      return false;
    }
  }
}