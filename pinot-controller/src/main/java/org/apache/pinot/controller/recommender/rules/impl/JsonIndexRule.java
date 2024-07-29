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
package org.apache.pinot.controller.recommender.rules.impl;

import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.configs.IndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** JSON columns must be NoDictionary columns with JsonIndex. */
public class JsonIndexRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonIndexRule.class);

  public JsonIndexRule(InputManager input, ConfigManager output) {
    super(input, output);
  }

  @Override
  public void run()
      throws InvalidInputException {
    int numColumns = _input.getNumCols();
    IndexConfig indexConfig = _output.getIndexConfig();
    for (int i = 0; i < numColumns; i++) {
      String columnName = _input.intToColName(i);
      FieldSpec.DataType columnType = _input.getFieldType(columnName);
      if (columnType == FieldSpec.DataType.JSON) {
        // JSON columns must be NoDictionary columns and have a JsonIndex.
        LOGGER.info("Recommending NoDictionary and JsonIndex on JSON column {}.", columnName);
        indexConfig.getJsonIndexColumns().add(columnName);
        indexConfig.getNoDictionaryColumns().add(columnName);
      }
    }
  }
}
