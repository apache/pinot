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
package org.apache.pinot.controller.recommender.rules.impl;

import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.PartitionRuleParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.DEFAULT_NUM_KAFKA_PARTITIONS;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.HYBRID;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.REALTIME;


/**
 * Recommend a number of kafka partitions if not provided
 * Divide the messages/sec (total aggregate in the topic) by 250 to get an optimal value of the number of kafka partitions
 */
public class KafkaPartitionRule extends AbstractRule {
  private final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionRule.class);
  PartitionRuleParams _params;

  public KafkaPartitionRule(InputManager input, ConfigManager output) {
    super(input, output);
    this._params = input.getPartitionRuleParams();
  }

  @Override
  public void run() {
    String tableType = _input.getTableType();
    if ((tableType.equalsIgnoreCase(HYBRID) || tableType
        .equalsIgnoreCase(REALTIME))) { //The table is real-time or hybrid
      if (_input.getNumKafkaPartitions()
          == DEFAULT_NUM_KAFKA_PARTITIONS)  // Recommend NumKafkaPartitions if it is not given
      {
        LOGGER.info("Recommending kafka partition configurations");
        LOGGER.info("*No kafka partition number found, recommending kafka partition number");
        _output.getPartitionConfig().setNumKafkaPartitions((int) Math
            .ceil((double) _input.getNumMessagesPerSecInKafkaTopic() / _params.KAFKA_NUM_MESSAGES_PER_SEC_PER_PARTITION));
        //Divide the messages/sec (total aggregate in the topic) by 250 to get an optimal value of the number of kafka partitions.
      }
      else{
        _output.getPartitionConfig().setNumKafkaPartitions(_input.getNumKafkaPartitions());
      }
    }
  }
}
