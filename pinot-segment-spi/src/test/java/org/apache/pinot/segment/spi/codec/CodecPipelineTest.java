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

import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;


/// Tests for [CodecPipeline] construction invariants.
public class CodecPipelineTest {

  @Test
  public void testDefensivelyCopiesStages() {
    CodecInvocation delta = new CodecInvocation("DELTA", List.of());
    List<CodecInvocation> callerStages = new ArrayList<>(List.of(delta));
    CodecPipeline pipeline = new CodecPipeline(callerStages);

    callerStages.set(0, new CodecInvocation("ZSTD", List.of("3")));
    assertSame(pipeline.stages().get(0), delta);
    assertThrows(UnsupportedOperationException.class, () -> pipeline.stages().clear());
  }

  @Test
  public void testRejectsInvalidStages() {
    assertThrows(NullPointerException.class, () -> new CodecPipeline(null));
    assertThrows(IllegalArgumentException.class, () -> new CodecPipeline(List.of()));
    assertThrows(NullPointerException.class,
        () -> new CodecPipeline(Arrays.asList((CodecInvocation) null)));
    assertThrows(IllegalArgumentException.class,
        () -> new CodecPipeline(Collections.nCopies(
            CodecSpecParser.MAX_PIPELINE_STAGES + 1, new CodecInvocation("DELTA", List.of()))));
  }
}
