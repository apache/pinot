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
package org.apache.pinot.common.datablock;

import java.util.List;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class MetadataBlockTest {

  @Test
  public void emptyMetadataBlock()
      throws Exception {
    // Given:
    // When:
    MetadataBlock metadataBlock = MetadataBlock.newEos();

    // Then:
    assertEquals(metadataBlock._fixedSizeData, PinotDataBuffer.empty());
    assertEquals(metadataBlock._variableSizeData, PinotDataBuffer.empty());

    List<DataBuffer> statsByStage = metadataBlock.getStatsByStage();
    assertNotNull(statsByStage, "Expected stats by stage to be non-null");
    assertEquals(statsByStage.size(), 0, "Expected no stats by stage");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void shouldThrowExceptionWhenUsingReadMethods() {
    // Given:
    MetadataBlock block = MetadataBlock.newEos();

    // When:
    // (should through exception)
    block.getInt(0, 0);
  }
}
