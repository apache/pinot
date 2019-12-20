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
package org.apache.pinot.druid.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import org.apache.pinot.druid.tools.hadoop.DruidToPinotSegmentConverterHadoopJob;
import org.testng.Assert;
import org.testng.annotations.Test;

// TODO: Restructure these tests once isDataFile is properly implemented.
//       The tests currently fail.
public class DruidSegmentFileValidatorTest {
  private static final File TEST_TAR_GZ = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_all_types.tar.gz"))
      .getFile());

  private static final File TEST_ZIP = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_all_types.zip"))
      .getFile());

  private static final File TEST_TAR_GZ_MISSING_FILE = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class
          .getClassLoader()
          .getResource("test_druid_all_types_missing_meta_smoosh.tar.gz"))
      .getFile());

  @Test
  public void testTarGZValidation() {
    boolean result = DruidToPinotSegmentConverterHadoopJob.isDataFileHelper(TEST_TAR_GZ.getPath());
    Assert.assertTrue(result);
  }

  @Test
  public void testZipValidation() {
    boolean result = DruidToPinotSegmentConverterHadoopJob.isDataFileHelper(TEST_ZIP.getPath());
    Assert.assertTrue(result);
  }

  @Test
  public void testValidationFalse() {
    boolean result = DruidToPinotSegmentConverterHadoopJob.isDataFileHelper(TEST_TAR_GZ_MISSING_FILE.getPath());
    Assert.assertFalse(result);
  }
}
