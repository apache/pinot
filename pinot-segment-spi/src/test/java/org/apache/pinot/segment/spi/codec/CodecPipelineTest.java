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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


public class CodecPipelineTest {

  @Test
  public void testDefensivelyCopiesStagesAndUsesWrapperlessDsl() {
    CodecInvocation delta = new CodecInvocation("DELTA", List.of());
    List<CodecInvocation> callerStages = new ArrayList<>(List.of(delta));
    CodecPipeline pipeline = new CodecPipeline(callerStages);

    callerStages.set(0, new CodecInvocation("ZSTD", List.of("3")));
    assertSame(pipeline.stages().get(0), delta);
    assertThrows(UnsupportedOperationException.class, () -> pipeline.stages().clear());
    assertEquals(new CodecPipeline(List.of(delta, new CodecInvocation("ZSTD", List.of("3")))).toDslString(),
        "DELTA,ZSTD(3)");
  }

  @Test
  public void testRejectsInvalidStages() {
    assertThrows(NullPointerException.class, () -> new CodecPipeline(null));
    assertThrows(IllegalArgumentException.class, () -> new CodecPipeline(List.of()));
    assertThrows(NullPointerException.class,
        () -> new CodecPipeline(Arrays.asList((CodecInvocation) null)));
  }

  @Test
  public void testEqualityFollowsTheCanonicalForm() {
    // Rewrite detection in later layers compares canonical specs, so a parsed pipeline and the equivalent
    // programmatically built one must be equal and agree on their hash code.
    CodecPipeline parsed = CodecSpecParser.parse(" delta , zstd( 3 ) ");
    CodecPipeline built = new CodecPipeline(
        List.of(new CodecInvocation("DELTA", List.of()), new CodecInvocation("ZSTD", List.of("3"))));
    assertEquals(built, parsed);
    assertEquals(built.hashCode(), parsed.hashCode());
    assertEquals(built.toDslString(), "DELTA,ZSTD(3)");
    assertEquals(built.toString(), built.toDslString());

    // Stage order is significant: a pipeline is an ordered list, not a set.
    assertNotEquals(built, new CodecPipeline(
        List.of(new CodecInvocation("ZSTD", List.of("3")), new CodecInvocation("DELTA", List.of()))));
    assertNotEquals(built, CodecSpecParser.parse("DELTA"));
    assertNotEquals(built, "DELTA,ZSTD(3)");
  }

  @Test
  public void testStageLimitMatchesTheParser() {
    // The parser and the constructor are independent entry points and must agree on the accept boundary.
    List<CodecInvocation> maxStages =
        Collections.nCopies(CodecSpecParser.MAX_PIPELINE_STAGES, new CodecInvocation("LZ4", List.of()));
    assertEquals(new CodecPipeline(maxStages).stages().size(), CodecSpecParser.MAX_PIPELINE_STAGES);
    assertEquals(
        CodecSpecParser.parse(String.join(",", Collections.nCopies(CodecSpecParser.MAX_PIPELINE_STAGES, "LZ4")))
            .stages().size(), CodecSpecParser.MAX_PIPELINE_STAGES);

    List<CodecInvocation> tooManyStages =
        Collections.nCopies(CodecSpecParser.MAX_PIPELINE_STAGES + 1, new CodecInvocation("LZ4", List.of()));
    assertThrows(IllegalArgumentException.class, () -> new CodecPipeline(tooManyStages));
  }
}
