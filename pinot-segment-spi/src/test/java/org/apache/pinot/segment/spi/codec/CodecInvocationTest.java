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
package org.apache.pinot.segment.spi.codec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class CodecInvocationTest {

  @Test
  public void testNormalizesNameAndDefensivelyCopiesArguments() {
    List<String> callerArgs = new ArrayList<>(List.of("3"));
    CodecInvocation invocation = new CodecInvocation("zstd", callerArgs);

    callerArgs.set(0, "8");
    assertEquals(invocation.name(), "ZSTD");
    assertEquals(invocation.args(), List.of("3"));
    assertThrows(UnsupportedOperationException.class, () -> invocation.args().add("8"));
  }

  @Test
  public void testRejectsInvalidStructure() {
    assertThrows(NullPointerException.class, () -> new CodecInvocation(null, List.of()));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("1ZSTD", List.of()));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("bad-name", List.of()));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("codec", List.of()));
    assertThrows(NullPointerException.class,
        () -> new CodecInvocation("ZSTD", Arrays.asList((String) null)));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("ZSTD", List.of("-1")));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("ZSTD", List.of("03")));
  }
}
