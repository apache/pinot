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
import com.google.common.io.Files;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.realtime.provisioning.MemoryEstimator;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.configs.SegmentSizeRecommendations;
import org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants;
import org.apache.pinot.controller.recommender.rules.io.params.SegmentSizeRuleParams;
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
 *
 * The purpose of generating a segment is to estimate the size and number of rows of one sample segment. In case user
 * already have a production segment in hand, they can provide actualSegmentSize and numRowsInActualSegment parameters
 * in the input and then this rule uses those parameters instead of generating a segment to derived those values.
 * It's worth noting that since this rule gets executed before other rules, we run into chicken-egg problem.
 *  - Index recommendation, dictionary, bloom filter rules will be run next.
 *  - Only after the subsequent rules run and the index recommendation is available, will we know what segment might
 *    look like.
 * So here we just assume dictionary on all dimensions and raw for metric columns and try to generate a segment quickly.
 * This also means that the calculated size of the optimal segment should ideally be bumped up by a factor, say 20%, to
 * account for what the size might look like when all the index/dictionary/bloom recommendations are applied.
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
      _output.setSegmentSizeRecommendations(new SegmentSizeRecommendations(
          "Segment sizing for realtime-only tables is done via Realtime Provisioning Rule"));
      return;
    }

    long segmentSize;
    int numRows;
    SegmentSizeRuleParams segmentSizeRuleParams = _input.getSegmentSizeRuleParams();
    if (segmentSizeRuleParams.getActualSegmentSizeMB() == RecommenderConstants.SegmentSizeRule.NOT_PROVIDED
        && segmentSizeRuleParams.getNumRowsInActualSegment() == RecommenderConstants.SegmentSizeRule.NOT_PROVIDED) {

      // generate a segment
      File workingDir = Files.createTempDir();
      try {
        TableConfig tableConfig = createTableConfig(_input.getSchema());
        int numRowsInGeneratedSegment = segmentSizeRuleParams.getNumRowsInGeneratedSegment();
        File generatedSegmentDir =
            new MemoryEstimator.SegmentGenerator(_input._schemaWithMetaData, _input._schema, tableConfig,
                numRowsInGeneratedSegment, true, workingDir).generate();
        segmentSize = Math.round(FileUtils.sizeOfDirectory(generatedSegmentDir) * RecommenderConstants.SegmentSizeRule.INDEX_OVERHEAD_RATIO_FOR_SEGMENT_SIZE);
        numRows = numRowsInGeneratedSegment;
      } finally {
        FileUtils.deleteQuietly(workingDir);
      }
    } else {
      segmentSize = segmentSizeRuleParams.getActualSegmentSizeMB() * MEGA_BYTE;
      numRows = segmentSizeRuleParams.getNumRowsInActualSegment();
    }

    // estimate optimal segment count & size parameters
    SegmentSizeRecommendations params =
        estimate(segmentSize, segmentSizeRuleParams.getDesiredSegmentSizeMB() * MEGA_BYTE, numRows,
            _input.getNumRecordsPerPush());

    // wire the recommendations and also update the cardinalities in input manager. The updated cardinalities are used in subsequent rules.
    _output.setSegmentSizeRecommendations(params);
    _input.capCardinalities((int) params.getNumRowsPerSegment());
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

  @Override
  public void hideOutput() {
    _output.setSegmentSizeRecommendations(null);
  }
}
