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

import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Recommend varied len dictionary on varied len data type (String/Byte type )
 */
public class VariedLengthDictionaryRule extends AbstractRule {
  private final Logger LOGGER = LoggerFactory.getLogger(VariedLengthDictionaryRule.class);

  public VariedLengthDictionaryRule(InputManager input, ConfigManager output) {
    super(input, output);
  }

  @Override
  public void run() {
    for (String colName : _input.getColNameToIntMap().keySet()) {
      if (!_output.getIndexConfig().getNoDictionaryColumns().contains(colName)) //exclude no dictionary column
      {
        LOGGER.debug("{} {}", _input.getFieldType(colName), colName);
        if (_input.getFieldType(colName) == FieldSpec.DataType.STRING
            || _input.getFieldType(colName) == FieldSpec.DataType.BYTES) {
          _output.getIndexConfig().getVariedLengthDictionaryColumns().add(colName);
        }
      }
    }
  }
}
