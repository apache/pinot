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
package org.apache.pinot.minion.taskfactory;

import java.util.Map;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TaskFactoryRegistryTest {

  @Test
  public void testSafeExceptionTypeDoesNotIncludeMessage() {
    String opaqueLiteral = "opaque-resolved-literal";
    IllegalStateException exception = new IllegalStateException(opaqueLiteral);

    String diagnostic = TaskFactoryRegistry.safeExceptionType(exception);

    Assert.assertEquals(diagnostic, IllegalStateException.class.getName());
    Assert.assertFalse(diagnostic.contains(opaqueLiteral));
  }

  @Test
  public void testTaskStartDiagnosticDoesNotIncludeConfigBody() {
    String opaqueLiteral = "opaque-resolved-literal";
    PinotTaskConfig taskConfig = new PinotTaskConfig("MaterializedViewTask",
        Map.of("definedSQL", "SELECT * FROM source WHERE value = '" + opaqueLiteral + "'"));

    String diagnostic = TaskFactoryRegistry.safeTaskStartDiagnostic(taskConfig, "task-id");

    Assert.assertEquals(diagnostic, "Start running MaterializedViewTask: task-id");
    Assert.assertFalse(diagnostic.contains(opaqueLiteral));
    Assert.assertFalse(diagnostic.contains("definedSQL"));
  }
}
