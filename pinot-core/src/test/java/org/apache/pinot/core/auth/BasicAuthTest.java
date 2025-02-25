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
package org.apache.pinot.core.auth;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BasicAuthTest {

  @Test
  public void testBasicAuthPrincipal() {
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("READ"))
        .hasTable("myTable"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Collections.emptySet(), ImmutableSet.of("Read"))
        .hasTable("myTable1"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasTable("myTable1"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable", "myTable1"),
        Collections.emptySet(), ImmutableSet.of("read"))
        .hasTable("myTable2"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", Collections.emptySet(), ImmutableSet.of("myTable"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), ImmutableSet.of("myTable1"),
        ImmutableSet.of("read"))
        .hasTable("myTable"));

    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("READ"))
        .hasPermission("read"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("Read"))
        .hasPermission("READ"));
    Assert.assertTrue(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasPermission("Read"));
    Assert.assertFalse(new BasicAuthPrincipal("name", "token", ImmutableSet.of("myTable"), Collections.emptySet(),
        ImmutableSet.of("read"))
        .hasPermission("write"));
  }
}
