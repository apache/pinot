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
package org.apache.pinot.sql.ddl.compile;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;


/// Behavioural tests for the new stateful {@link DdlCompiler#compile(String,
/// DdlCompileContext)} entry point and its STATELESS shim.
public class DdlCompileContextTest {

  @Test
  public void statelessSingletonHasNullTableCache() {
    assertNull(DdlCompileContext.STATELESS.tableCache(),
        "STATELESS context must report a null TableCache so DDL paths that need one can "
            + "produce an actionable error rather than NPE-ing inside the inferer.");
  }

  // ----------------------------------------------------------------------------------------------
  // DdlCompiler entry-point semantics
  // ----------------------------------------------------------------------------------------------

  @Test
  public void newEntryRejectsNullContext() {
    DdlCompilationException ex = expectThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("DROP TABLE foo", null));
    org.assertj.core.api.Assertions.assertThat(ex.getMessage())
        .contains("DdlCompileContext must not be null")
        .contains("DdlCompileContext.STATELESS");
  }

  @Test
  public void newEntryAcceptsTextualDdlWithStatelessContext() {
    // Sanity-check that the DDL forms which never need cluster state (every form except
    // future MV inference) still compile cleanly with the migration target STATELESS.
    CompiledDdl compiled = DdlCompiler.compile("DROP TABLE IF EXISTS foo", DdlCompileContext.STATELESS);
    assertNotNull(compiled);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void deprecatedEntryDelegatesToStatelessContext() {
    // The deprecated overload exists purely as a compatibility shim. Its behaviour MUST
    // match `compile(sql, STATELESS)` exactly so existing callers can migrate without
    // any observable change. We compare on the operation type rather than equality
    // because CompiledDdl subclasses don't override equals().
    CompiledDdl viaDeprecated = DdlCompiler.compile("DROP TABLE IF EXISTS foo");
    CompiledDdl viaStateless = DdlCompiler.compile("DROP TABLE IF EXISTS foo", DdlCompileContext.STATELESS);
    org.testng.Assert.assertEquals(viaDeprecated.getOperation(), viaStateless.getOperation());
  }

  @Test
  public void newEntryRejectsEmptySql() {
    // The original entry point's failure modes must stay reachable through the new entry.
    assertThrows(DdlCompilationException.class,
        () -> DdlCompiler.compile("", DdlCompileContext.STATELESS));
  }
}
