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
package org.apache.pinot.tools.admin.command;

import java.lang.reflect.InvocationTargetException;
import org.apache.pinot.tools.EmptyQuickstart;
import org.apache.pinot.tools.HybridQuickstart;
import org.apache.pinot.tools.MultiClusterQuickstart;
import org.apache.pinot.tools.QuickStartBase;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.RealtimeQuickStart;
import org.apache.pinot.tools.RealtimeQuickStartWithMinion;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class QuickStartTest {

  @Test(expectedExceptions = UnsupportedOperationException.class,
      expectedExceptionsMessageRegExp = "^No QuickStart type provided. Valid types are: \\[.*\\]$")
  public void testNoArg()
      throws Exception {
    QuickStartCommand quickStartCommand = new QuickStartCommand();
    quickStartCommand.execute();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class,
      expectedExceptionsMessageRegExp = "^Unsupported QuickStart type: foo. Valid types are: \\[.*\\]$")
  public void testInvalidQuickStart()
      throws Exception {
    QuickStartCommand quickStartCommand = new QuickStartCommand();
    quickStartCommand.setType("foo");
    quickStartCommand.execute();
  }

  @Test
  public void testMatchStringToCommand()
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    assertEquals(quickStartClassFor("OFFLINE"), Quickstart.class);
    assertEquals(quickStartClassFor("offline"), Quickstart.class);
    assertEquals(quickStartClassFor("BATCH"), Quickstart.class);

    assertEquals(quickStartClassFor("EMPTY"), EmptyQuickstart.class);
    assertEquals(quickStartClassFor("DEFAULT"), EmptyQuickstart.class);

    assertEquals(quickStartClassFor("REALTIME_MINION"), RealtimeQuickStartWithMinion.class);
    assertEquals(quickStartClassFor("REALTIME-MINION"), RealtimeQuickStartWithMinion.class);

    assertEquals(quickStartClassFor("REALTIME"), RealtimeQuickStart.class);
    assertEquals(quickStartClassFor("STREAM"), RealtimeQuickStart.class);

    assertEquals(quickStartClassFor("HYBRID"), HybridQuickstart.class);

    assertEquals(quickStartClassFor("MULTI_CLUSTER"), MultiClusterQuickstart.class);
    assertEquals(quickStartClassFor("MULTICLUSTER"), MultiClusterQuickstart.class);
  }

  /// The batch-side quickstarts that only differed by their sample queries were merged into [Quickstart]; their types
  /// must keep resolving so that existing docs and scripts do not break.
  @Test
  public void testDeprecatedBatchTypesResolveToQuickstart()
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    for (String type : new String[]{
        "MULTI_STAGE", "JOIN", "TIMESTAMP", "OFFLINE_JSON_INDEX", "OFFLINE-JSON-INDEX", "BATCH_JSON_INDEX",
        "BATCH-JSON-INDEX", "OFFLINE_COMPLEX_TYPE", "OFFLINE-COMPLEX-TYPE", "BATCH_COMPLEX_TYPE", "BATCH-COMPLEX-TYPE"
    }) {
      assertEquals(quickStartClassFor(type), Quickstart.class, type);
      assertTrue(new Quickstart().deprecatedTypes().contains(type), type);
    }
  }

  /// The stream-side quickstarts that only differed by their sample queries were merged into [RealtimeQuickStart];
  /// their types must keep resolving so that existing docs and scripts do not break.
  @Test
  public void testDeprecatedStreamTypesResolveToRealtimeQuickStart()
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    for (String type : new String[]{
        "UPSERT", "UPSERT_JSON_INDEX", "UPSERT-JSON-INDEX", "PARTIAL_UPSERT", "PARTIAL-UPSERT", "REALTIME_JSON_INDEX",
        "REALTIME-JSON-INDEX", "STREAM_JSON_INDEX", "STREAM-JSON-INDEX", "REALTIME_COMPLEX_TYPE",
        "REALTIME-COMPLEX-TYPE", "STREAM_COMPLEX_TYPE", "STREAM-COMPLEX-TYPE"
    }) {
      assertEquals(quickStartClassFor(type), RealtimeQuickStart.class, type);
      assertTrue(new RealtimeQuickStart().deprecatedTypes().contains(type), type);
    }
  }

  /// Every deprecated alias must be declared by the quickstart it resolves to, and must not shadow a canonical type.
  @Test
  public void testDeprecatedTypesAreASubsetOfTypes() {
    assertTrue(new Quickstart().types().containsAll(new Quickstart().deprecatedTypes()));
    assertTrue(new RealtimeQuickStart().types().containsAll(new RealtimeQuickStart().deprecatedTypes()));
    assertFalse(new Quickstart().deprecatedTypes().contains("BATCH"));
    assertFalse(new RealtimeQuickStart().deprecatedTypes().contains("REALTIME"));
  }

  private Class<? extends QuickStartBase> quickStartClassFor(String type)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    return new QuickStartCommand().selectQuickStart(type).getClass();
  }
}
