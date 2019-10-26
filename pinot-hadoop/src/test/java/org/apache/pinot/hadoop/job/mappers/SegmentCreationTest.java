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
package org.apache.pinot.hadoop.job.mappers;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentCreationTest {

  @Test
  public void testBootstrapOutputPath() {
    URI baseDir = new Path("/path/to/input").toUri();
    URI inputFile = new Path("/path/to/input/a/b/c/d/e").toUri();
    Path outputPath = new Path("/path/to/output");
    Path relativeOutputPath = SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath);
    Assert.assertEquals(relativeOutputPath.toString(), "/path/to/output/a/b/c/d");
  }

  @Test
  public void testBootstrapOutputPath1() {
    URI baseDir = new Path("/path/to/input/*/*.avro").toUri();
    URI inputFile = new Path("/path/to/input/a/b.avro").toUri();
    Path outputPath = new Path("/path/to/output");
    Assert.assertThrows(() -> SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath));
  }

  @Test
  public void testBootstrapOutputPathS3() {
    URI baseDir = new Path("s3a://sample-s3-bucket/tmp/pinot/input").toUri();
    URI inputFile = new Path("s3a://sample-s3-bucket/tmp/pinot/input/airlineStats_data.avro").toUri();
    Path outputPath = new Path("s3a://sample-s3-bucket/tmp/pinot/output");
    Path relativeOutputPath = SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath);
    Assert.assertEquals(relativeOutputPath.toString(), "s3a://sample-s3-bucket/tmp/pinot/output");

    baseDir = new Path("s3a://sample-s3-bucket/tmp/pinot/input").toUri();
    inputFile = new Path("s3a://sample-s3-bucket/tmp/pinot/input/yyyy=2019/mm=10/dd=18/airlineStats_data.avro").toUri();
    outputPath = new Path("s3a://sample-s3-bucket/tmp/pinot/output");
    relativeOutputPath = SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath);
    Assert.assertEquals(relativeOutputPath.toString(), "s3a://sample-s3-bucket/tmp/pinot/output/yyyy=2019/mm=10/dd=18");
  }

  @Test
  public void testBootstrapOutputPathHdfs() {
    URI baseDir = new Path("hdfs://raw-data/tmp/pinot/input").toUri();
    URI inputFile = new Path("hdfs://raw-data/tmp/pinot/input/airlineStats_data.avro").toUri();
    Path outputPath = new Path("hdfs://raw-data/tmp/pinot/output");
    Path relativeOutputPath = SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath);
    Assert.assertEquals(relativeOutputPath.toString(), "hdfs://raw-data/tmp/pinot/output");

    baseDir = new Path("hdfs://raw-data/tmp/pinot/input").toUri();
    inputFile = new Path("hdfs://raw-data/tmp/pinot/input/yyyy=2019/mm=10/dd=18/airlineStats_data.avro").toUri();
    outputPath = new Path("hdfs://raw-data/tmp/pinot/output");
    relativeOutputPath = SegmentCreationMapper.getRelativeOutputPath(baseDir, inputFile, outputPath);
    Assert.assertEquals(relativeOutputPath.toString(), "hdfs://raw-data/tmp/pinot/output/yyyy=2019/mm=10/dd=18");
  }
}
