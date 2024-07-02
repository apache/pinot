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
package org.apache.pinot.query.runtime.blocks;

import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStatsTest;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TransferableBlockTest {

  @Test(dataProviderClass = MultiStageQueryStatsTest.class, dataProvider = "stats")
  public void serializeDeserialize(MultiStageQueryStats queryStats) {
    TransferableBlock transferableBlock = new TransferableBlock(queryStats);
    List<DataBuffer> fromStatsBytes = transferableBlock.getSerializedStatsByStage();

    DataBlock dataBlock = transferableBlock.getDataBlock();

    TransferableBlock fromBlock = TransferableBlockUtils.wrap(dataBlock);
    List<DataBuffer> fromBlockBytes = fromBlock.getSerializedStatsByStage();

    Assert.assertEquals(fromStatsBytes, fromBlockBytes, "Serialized bytes from stats and block should be equal");
  }
}
