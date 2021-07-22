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
package org.apache.pinot.hadoop.job.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.pinot.ingestion.preprocess.mappers.OrcDataPreprocessingMapper;
import org.apache.pinot.ingestion.preprocess.reducers.OrcDataPreprocessingReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HadoopOrcDataPreprocessingHelper extends HadoopDataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopOrcDataPreprocessingHelper.class);

  public HadoopOrcDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    super(dataPreprocessingHelper);
  }

  @Override
  public void setUpMapperReducerConfigs(Job job)
      throws IOException {
    Object orcSchema = getSchema(_dataPreprocessingHelper._sampleRawDataPath);
    String orcSchemaString = orcSchema.toString();
    LOGGER.info("Orc schema is: {}", orcSchemaString);
    validateConfigsAgainstSchema(orcSchema);

    job.setInputFormatClass(OrcInputFormat.class);
    job.setMapperClass(OrcDataPreprocessingMapper.class);
    job.setMapOutputValueClass(OrcValue.class);
    Configuration jobConf = job.getConfiguration();
    OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(jobConf, orcSchemaString);

    job.setReducerClass(OrcDataPreprocessingReducer.class);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, OrcOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcStruct.class);
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(jobConf, orcSchemaString);
  }
}
