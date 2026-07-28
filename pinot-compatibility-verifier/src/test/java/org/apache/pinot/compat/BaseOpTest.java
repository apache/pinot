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
package org.apache.pinot.compat;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests capability-gated compatibility operations.
 */
public class BaseOpTest {
  private static final String TEST_PROPERTY = BaseOpTest.class.getName() + ".enabled";

  @Test
  public void testSystemPropertyRunCondition() {
    String previousValue = System.getProperty(TEST_PROPERTY);
    try {
      CountingOp op = new CountingOp();
      assertTrue(op.run(1));
      assertEquals(op._runCount, 1);

      op.setRunIfSystemProperty(TEST_PROPERTY);
      assertFalse(op.run(1));
      assertEquals(op._runCount, 1);

      op.setRunIfSystemPropertyValue("true");
      System.clearProperty(TEST_PROPERTY);
      assertFalse(op.run(1));
      assertEquals(op._runCount, 1);

      System.setProperty(TEST_PROPERTY, "false");
      assertTrue(op.run(1));
      assertEquals(op._runCount, 1);

      System.setProperty(TEST_PROPERTY, "true");
      assertTrue(op.run(1));
      assertEquals(op._runCount, 2);
    } finally {
      if (previousValue == null) {
        System.clearProperty(TEST_PROPERTY);
      } else {
        System.setProperty(TEST_PROPERTY, previousValue);
      }
    }
  }

  private static final class CountingOp extends BaseOp {
    private int _runCount;

    private CountingOp() {
      super(OpType.QUERY_OP);
    }

    @Override
    boolean runOp(int generationNumber) {
      _runCount++;
      return true;
    }
  }
}
