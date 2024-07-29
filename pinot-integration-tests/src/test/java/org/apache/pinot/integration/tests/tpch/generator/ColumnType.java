package org.apache.pinot.integration.tests.tpch.generator;

import java.util.List;


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
public enum ColumnType {
  NUMERIC(List.of("=", "!=", "<", ">", "<=", ">=", "<>"), List.of("max", "min", "avg", "sum", "count")),
  STRING(List.of("=", "!=", "LIKE"), List.of());

  List<String> _operators;
  List<String> _aggregations;

  ColumnType(List<String> operators, List<String> aggregateFunctions) {
    _operators = operators;
    _aggregations = aggregateFunctions;
  }
}
