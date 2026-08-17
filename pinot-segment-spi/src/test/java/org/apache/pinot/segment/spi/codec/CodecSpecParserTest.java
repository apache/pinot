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

import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class CodecSpecParserTest {

  @Test
  public void testSingleInvocation() {
    CodecPipeline pipeline = CodecSpecParser.parse("zstd(3)");

    assertEquals(pipeline.stages().size(), 1);
    assertEquals(pipeline.stages().get(0).name(), "ZSTD");
    assertEquals(pipeline.stages().get(0).args(), List.of("3"));
    assertEquals(pipeline.toDslString(), "ZSTD(3)");
  }

  @Test
  public void testWrapperlessPipelineAndWhitespaceNormalization() {
    CodecPipeline pipeline = CodecSpecParser.parse(" delta , zstd( 3 ) , lz4 ");

    assertEquals(pipeline.stages().size(), 3);
    assertEquals(pipeline.toDslString(), "DELTA,ZSTD(3),LZ4");
  }

  @Test
  public void testRemovedWrapperIsRejected() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse("CODEC(DELTA,ZSTD(3))"));
    assertTrue(exception.getMessage().contains("wrapper is not supported"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("codec()"));
  }

  @Test
  public void testInvalidSpecsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse(null));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("  "));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("ZSTD(3"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("DELTA extra"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("ZSTD(abc)"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("ZSTD(+3)"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("ZSTD(-3)"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("ZSTD(०)"));
    assertThrows(IllegalArgumentException.class, () -> CodecSpecParser.parse("DELTA,"));
  }

  @Test
  public void testStructuralLimitsAreEnforced() {
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse("A".repeat(CodecSpecParser.MAX_SPEC_LENGTH + 1)));
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse(" ".repeat(CodecSpecParser.MAX_SPEC_LENGTH) + "LZ4"));
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse("A".repeat(CodecSpecParser.MAX_IDENTIFIER_LENGTH + 1)));
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse("ZSTD(" + "1".repeat(CodecSpecParser.MAX_ARGUMENT_LENGTH + 1) + ")"));
    String maximumArguments = String.join(",",
        Collections.nCopies(CodecSpecParser.MAX_ARGS_PER_INVOCATION, "1"));
    assertEquals(CodecSpecParser.parse("TEST(" + maximumArguments + ")").stages().get(0).args().size(),
        CodecSpecParser.MAX_ARGS_PER_INVOCATION);
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse("TEST(" + maximumArguments + ",1)"));
    assertThrows(IllegalArgumentException.class,
        () -> CodecSpecParser.parse(String.join(",",
            Collections.nCopies(CodecSpecParser.MAX_PIPELINE_STAGES + 1, "LZ4"))));
  }
}
