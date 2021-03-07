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
package org.apache.pinot.util;

import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.core.util.PinotConfigSchemaValidator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotConfigSchemaValidatorTest {

  @Test
  public void testValidateSchema()
      throws IOException, ProcessingException {

    PinotConfigSchemaValidator validator = PinotConfigSchemaValidator.forTableConfig();

    // given a valid schema, validate should returns true with not error messages
    String airlinesTableConfig = JsonLoader.fromResource("/schemas/good_airlines_table_config.json").toString();
    List<String> result = validator.validate(airlinesTableConfig);
    Assert.assertEquals(result.size(), 0);

    // given a invalid schema [invalid key is table_type], validate should returns false
    airlinesTableConfig = JsonLoader.fromResource("/schemas/bad_airlines_table_config.json").toString();
    result = validator.validate(airlinesTableConfig);
    Assert.assertTrue(result.size() > 0);
  }
}
