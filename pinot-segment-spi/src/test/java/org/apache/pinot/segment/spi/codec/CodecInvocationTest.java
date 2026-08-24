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
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
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

  @Test
  public void testEqualityFollowsTheCanonicalForm() {
    // Later layers freeze the canonical spec into segment headers and compare it to detect rewrites, so two
    // spellings of the same invocation must be equal and agree on their hash code.
    CodecInvocation parsed = CodecSpecParser.parse("zstd( 3 )").stages().get(0);
    CodecInvocation built = new CodecInvocation("ZSTD", List.of("3"));
    assertEquals(built, parsed);
    assertEquals(built.hashCode(), parsed.hashCode());
    assertEquals(built.toDslString(), "ZSTD(3)");
    assertEquals(built.toString(), "ZSTD(3)");

    assertNotEquals(built, new CodecInvocation("ZSTD", List.of("4")));
    assertNotEquals(built, new CodecInvocation("LZ4", List.of("3")));
    assertNotEquals(built, new CodecInvocation("ZSTD", List.of()));
    assertNotEquals(built, "ZSTD(3)");

    // An argument-less invocation renders without parentheses, so "DELTA()" and "DELTA" canonicalize alike.
    CodecInvocation noArgs = new CodecInvocation("DELTA", List.of());
    assertEquals(noArgs.toString(), "DELTA");
    assertEquals(CodecSpecParser.parse("DELTA()").stages().get(0), noArgs);
    assertEquals(CodecSpecParser.parse("DELTA").stages().get(0), noArgs);
  }

  @Test
  public void testStructuralLimitsMatchTheParser() {
    // Codec definitions build invocations programmatically, bypassing the parser, so both entry points must
    // enforce the same bounds — on the accept side as well as the reject side.
    String maxName = "A".repeat(CodecSpecParser.MAX_IDENTIFIER_LENGTH);
    assertEquals(new CodecInvocation(maxName, List.of()).name(), maxName);
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation(maxName + "A", List.of()));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("", List.of()));

    String maxArg = "1".repeat(CodecSpecParser.MAX_ARGUMENT_LENGTH);
    assertEquals(new CodecInvocation("ZSTD", List.of(maxArg)).args(), List.of(maxArg));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("ZSTD", List.of(maxArg + "1")));
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("ZSTD", List.of("")));

    List<String> maxArgs = Collections.nCopies(CodecSpecParser.MAX_ARGS_PER_INVOCATION, "1");
    assertEquals(new CodecInvocation("ZSTD", maxArgs).args().size(), CodecSpecParser.MAX_ARGS_PER_INVOCATION);
    List<String> tooManyArgs = Collections.nCopies(CodecSpecParser.MAX_ARGS_PER_INVOCATION + 1, "1");
    assertThrows(IllegalArgumentException.class, () -> new CodecInvocation("ZSTD", tooManyArgs));

    assertThrows(NullPointerException.class, () -> new CodecInvocation("ZSTD", null));
  }
}
