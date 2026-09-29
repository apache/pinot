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
package org.apache.pinot.sql.parsers.rewriter;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Pins the `RlsUtils.isRlsAppliedForTable` contract used by the broker to stamp
/// `BrokerResponse.setRLSFiltersApplied(...)`.  Future refactors that break any of the
/// short-circuits would silently flip the response field for every query.
public class RlsUtilsTest {

  @Test
  public void testIsRlsAppliedForTableNullMap() {
    Assert.assertFalse(RlsUtils.isRlsAppliedForTable(null, "t"));
  }

  @Test
  public void testIsRlsAppliedForTableEmptyMap() {
    Assert.assertFalse(RlsUtils.isRlsAppliedForTable(new HashMap<>(), "t"));
  }

  @Test
  public void testIsRlsAppliedForTableMissingKey() {
    Map<String, String> opts = new HashMap<>();
    opts.put("rlsFilters-other", "( x = 1 )");
    Assert.assertFalse(RlsUtils.isRlsAppliedForTable(opts, "t"));
  }

  @Test
  public void testIsRlsAppliedForTableEmptyValue() {
    Map<String, String> opts = new HashMap<>();
    opts.put(RlsUtils.buildRlsFilterKey("t"), "");
    Assert.assertFalse(RlsUtils.isRlsAppliedForTable(opts, "t"),
        "Empty filter value must read as not-applied — matches RlsFiltersRewriter's own short-circuit");
  }

  @Test
  public void testIsRlsAppliedForTableNonEmptyValue() {
    Map<String, String> opts = new HashMap<>();
    opts.put(RlsUtils.buildRlsFilterKey("t"), "( x = 1 )");
    Assert.assertTrue(RlsUtils.isRlsAppliedForTable(opts, "t"));
  }

  @Test
  public void testIsRlsAppliedForTableDifferentTable() {
    Map<String, String> opts = new HashMap<>();
    opts.put(RlsUtils.buildRlsFilterKey("t"), "( x = 1 )");
    Assert.assertFalse(RlsUtils.isRlsAppliedForTable(opts, "other"),
        "Marker for table `t` must not register as applied for table `other`");
  }
}
