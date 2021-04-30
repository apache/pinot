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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.realtime.provisioning.MemoryEstimator;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.configs.SegmentSizeRecommendations;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * This rule generates a segment based on the provided schema characteristics and then recommends the followings
 * using the size and number of records in the generated segments:
 *   - number of segments
 *   - number of records in each segment
 *   - size of each segment
 */
public class SegmentSizeRule extends AbstractRule {

  static final int MEGA_BYTE = 1024 * 1024;

  public SegmentSizeRule(InputManager input, ConfigManager output) {
    super(input, output);
  }

  @Override
  public void run()
      throws InvalidInputException {

    if (_input.getTableType().equalsIgnoreCase("REALTIME")) {
      // no need to estimate segment size & optimal number of segments for realtime only tables;
      // RT Provisioning Rule will have a comprehensive analysis on that
      return;
    }

    // generate a segment
    TableConfig tableConfig = createTableConfig(_input.getSchema());
    int numRowsInGeneratedSegment = _input.getSegmentSizeRuleParams().getNumRowsInGeneratedSegment();
    File generatedSegmentDir =
        new MemoryEstimator.SegmentGenerator(_input._schemaWithMetaData, _input._schema, tableConfig,
            numRowsInGeneratedSegment, true).generate();

    // estimate optimal segment count & size parameters
    SegmentSizeRecommendations params = estimate(FileUtils.sizeOfDirectory(generatedSegmentDir),
        _input.getSegmentSizeRuleParams().getDesiredSegmentSizeMb() * MEGA_BYTE, numRowsInGeneratedSegment,
        _input.getNumRecordsPerPush());

    // wire the recommendations
    _output.setSegmentSizeRecommendations(params);
    _input.capCardinalities((int) params.getNumRows());

    // cleanup
    try {
      FileUtils.deleteDirectory(generatedSegmentDir);
    } catch (IOException e) {
      throw new RuntimeException("Cannot delete the generated segment directory", e);
    }
  }

  /**
   * Estimate segment size parameters by extrapolation based on the number of records and size of the generated segment.
   * The linear extrapolation used here is not optimal because of columnar way of storing data and usage of different
   * indices. Another way would be to iteratively generate new segments with expected number of rows until the ideal
   * segment is found, but that's costly because of the time it takes to generate segments. Although the extrapolation
   * approach seems to be less accurate, it is chosen due to its performance.
   *
   * @param GSS  generated segment size
   * @param DSS  desired segment size
   * @param NRGS num records of generated segment
   * @param NRPP num records per push
   * @return recommendations on optimal segment count, size, and number of records
   */
  @VisibleForTesting
  SegmentSizeRecommendations estimate(long GSS, int DSS, int NRGS, long NRPP) {

    // calc num rows in desired segment
    double sizeRatio = (double) DSS / GSS;
    long numRowsInDesiredSegment = Math.round(NRGS * sizeRatio);

    // calc optimal num segment
    long optimalNumSegments = Math.round(NRPP / (double) numRowsInDesiredSegment);
    optimalNumSegments = Math.max(optimalNumSegments, 1);

    // revise optimal num rows in segment
    long optimalNumRowsInSegment = Math.round(NRPP / (double) optimalNumSegments);

    // calc optimal segment size
    double rowRatio = (double) optimalNumRowsInSegment / NRGS;
    long optimalSegmentSize = Math.round(GSS * rowRatio);

    return new SegmentSizeRecommendations(optimalNumRowsInSegment, optimalNumSegments, optimalSegmentSize);
  }

  private TableConfig createTableConfig(Schema schema) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(schema.getSchemaName())
        .setNoDictionaryColumns(schema.getMetricNames())
        .build();
  }
}
