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
package org.apache.pinot.hadoop;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Properties;
import org.apache.pinot.hadoop.job.SegmentPreprocessingJob;
import org.apache.pinot.hadoop.job.SegmentCreationJob;
import org.apache.pinot.hadoop.job.SegmentTarPushJob;
import org.apache.pinot.hadoop.job.SegmentUriPushJob;


public class PinotHadoopJobLauncher {

  enum PinotHadoopJobType {
    SegmentCreation, SegmentTarPush, SegmentUriPush, SegmentCreationAndTarPush, SegmentCreationAndUriPush,
    SegmentPreprocessing
  }

  private static final String USAGE = "usage: [job_type] [job.properties]";
  private static final String SUPPORT_JOB_TYPES =
      "\tsupport job types: " + Arrays.toString(PinotHadoopJobType.values());

  private static void usage() {
    System.err.println(USAGE);
    System.err.println(SUPPORT_JOB_TYPES);
  }

  private static void kickOffPinotHadoopJob(PinotHadoopJobType jobType, Properties jobConf)
      throws Exception {
    switch (jobType) {
      case SegmentCreation:
        new SegmentCreationJob(jobConf).run();
        break;
      case SegmentTarPush:
        new SegmentTarPushJob(jobConf).run();
        break;
      case SegmentUriPush:
        new SegmentUriPushJob(jobConf).run();
        break;
      case SegmentCreationAndTarPush:
        new SegmentCreationJob(jobConf).run();
        new SegmentTarPushJob(jobConf).run();
        break;
      case SegmentCreationAndUriPush:
        new SegmentCreationJob(jobConf).run();
        new SegmentUriPushJob(jobConf).run();
        break;
      case SegmentPreprocessing:
        new SegmentPreprocessingJob(jobConf).run();
        break;
      default:
        throw new RuntimeException("Not a valid jobType - " + jobType);
    }
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }
    PinotHadoopJobType jobType = null;
    Properties jobConf = null;
    try {
      jobType = PinotHadoopJobType.valueOf(args[0]);
      jobConf = new Properties();
      jobConf.load(new FileInputStream(args[1]));
    } catch (Exception e) {
      usage();
      System.exit(1);
    }
    kickOffPinotHadoopJob(jobType, jobConf);
  }
}
