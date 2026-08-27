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
package org.apache.pinot.controller.recommender.data.generator;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class GeneratorFactoryTest {

  /// VARIANT must be rejected explicitly rather than falling through to the numeric generators, which would emit
  /// values that are not valid Variant envelopes.
  @Test
  public void testVariantIsRejectedExplicitly() {
    UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class,
        () -> GeneratorFactory.getGeneratorFor(DataType.VARIANT, 10, 1.0, 10, null));
    assertTrue(exception.getMessage().contains("VARIANT"), exception.getMessage());
  }
}
