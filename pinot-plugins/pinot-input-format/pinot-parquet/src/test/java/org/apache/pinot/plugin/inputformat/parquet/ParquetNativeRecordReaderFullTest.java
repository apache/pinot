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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.net.URLDecoder;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;


public class ParquetNativeRecordReaderFullTest {
  protected final File _tempDir = new File(FileUtils.getTempDirectory(), "ParquetNativeRecordReaderFullTest");

  @Test
  protected void testReadDataSet1()
      throws Exception {
    testParquetFile("airlineStats.zstd.parquet");
    testParquetFile("baseballStats.zstd.parquet");
    testParquetFile("test-data/gzip-nation.impala.parquet");
    //testParquetFile("test-data/nation.dict.parquet");
    testParquetFile("test-data/nation.impala.parquet");
    testParquetFile("test-data/nation.plain.parquet");
    testParquetFile("test-data/snappy-nation.impala.parquet");
    testParquetFile("test-data/test-converted-type-null.parquet");
    testParquetFile("test-data/test-null.parquet");
    testParquetFile("test-data/test-null-dictionary.parquet");
    testParquetFile("test-data/airlines_parquet/4345e5eef217aa1b-c8f16177f35fd983_1150363067_data.1.parq");
    testParquetFile("test-data/dir_metadata/empty.parquet");
    testParquetFile("test-data/multi_rgs_pyarrow/b=hi/a97cc141d16f4014a59e5b234dddf07c.parquet");
    testParquetFile("test-data/multi_rgs_pyarrow/b=lo/01bc139247874a0aa9e0e541f2eec497.parquet");
    for (int i = 1; i < 4; i++) {
      for (int j = 0; j < 8; j++) {
        testParquetFile("test-data/split/cat=fred/catnum=" + i + "/part-r-0000" + j
            + "-4805f816-a859-4b75-8659-285a6617386f.gz.parquet");
        testParquetFile("test-data/split/cat=freda/catnum=" + i + "/part-r-0000" + j
            + "-4805f816-a859-4b75-8659-285a6617386f.gz.parquet");
      }
    }

    testParquetFile("test-data/customer.impala.parquet");
    testParquetFile("test-data/datapage_v2.snappy.parquet");
    testParquetFile("test-data/decimals.parquet");
    testParquetFile("test-data/empty.parquet");
    testParquetFile("test-data/foo.parquet");
    testParquetFile("test-data/gzip-nation.impala.parquet");
    testParquetFile("test-data/map-test.snappy.parquet");
    testParquetFile("test-data/map_array.parq");
    testParquetFile("test-data/metas.parq");
    testParquetFile("test-data/mr_times.parq");
    testParquetFile("test-data/nested.parq");
    testParquetFile("test-data/nested1.parquet");
    //testParquetFile("test-data/no_columns.parquet");
    testParquetFile("test-data/non-std-kvm.fp-0.8.2.parquet");
    testParquetFile("test-data/repeated_no_annotation.parquet");
    testParquetFile("test-data/snappy-nation.impala.parquet");
    testParquetFile("test-data/test.parquet");
    testParquetFile("test-data/test-converted-type-null.parquet");
    testParquetFile("test-data/test-map-last-row-split.parquet");
    testParquetFile("test-data/test-null.parquet");
    testParquetFile("test-data/test-null-dictionary.parquet");
  }

  @Test
  protected void testReadDataSet2()
      throws Exception {
    testParquetFile("githubEvents.snappy.parquet");
    testParquetFile("test-data2/alltypes_dictionary.parquet");
    testParquetFile("test-data2/alltypes_plain.parquet");
    testParquetFile("test-data2/alltypes_plain.snappy.parquet");
    testParquetFile("test-data2/alltypes_tiny_pages.parquet");
    testParquetFile("test-data2/alltypes_tiny_pages_plain.parquet");
    testParquetFile("test-data2/binary.parquet");
    testParquetFile("test-data2/byte_array_decimal.parquet");
    testParquetFile("test-data2/data_index_bloom_encoding_stats.parquet");
    testParquetFile("test-data2/datapage_v2.snappy.parquet");
    testParquetFile("test-data2/delta_binary_packed.parquet");
    testParquetFile("test-data2/delta_byte_array.parquet");
    testParquetFile("test-data2/delta_encoding_optional_column.parquet");
    testParquetFile("test-data2/delta_encoding_required_column.parquet");
    //testParquetFile("test-data2/delta_length_byte_array.parquet");
    testParquetFile("test-data2/dict-page-offset-zero.parquet");
    //testParquetFile("test-data2/encrypt_columns_and_footer.parquet.encrypted");
    //testParquetFile("test-data2/encrypt_columns_and_footer_aad.parquet.encrypted");
    //testParquetFile("test-data2/encrypt_columns_and_footer_ctr.parquet.encrypted");
    //testParquetFile("test-data2/encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted");
    //testParquetFile("test-data2/encrypt_columns_plaintext_footer.parquet.encrypted");
    testParquetFile("test-data2/fixed_length_decimal.parquet");
    testParquetFile("test-data2/fixed_length_decimal_legacy.parquet");
    //testParquetFile("test-data2/hadoop_lz4_compressed.parquet");
    //testParquetFile("test-data2/hadoop_lz4_compressed_larger.parquet");
    testParquetFile("test-data2/int32_decimal.parquet");
    testParquetFile("test-data2/int64_decimal.parquet");
    testParquetFile("test-data2/list_columns.parquet");
    //testParquetFile("test-data2/lz4_raw_compressed.parquet");
    //testParquetFile("test-data2/lz4_raw_compressed_larger.parquet");
    //testParquetFile("test-data2/nation.dict-malformed.parquet");
    testParquetFile("test-data2/nested_lists.snappy.parquet");
    testParquetFile("test-data2/nested_maps.snappy.parquet");
    //testParquetFile("test-data2/nested_structs.rust.parquet");
    //testParquetFile("test-data2/non_hadoop_lz4_compressed.parquet");
    testParquetFile("test-data2/nonnullable.impala.parquet");
    testParquetFile("test-data2/null_list.parquet");
    testParquetFile("test-data2/nullable.impala.parquet");
    testParquetFile("test-data2/nulls.snappy.parquet");
    testParquetFile("test-data2/repeated_no_annotation.parquet");
    testParquetFile("test-data2/single_nan.parquet");
    //testParquetFile("test-data2/uniform_encryption.parquet.encrypted");
  }

  protected void testParquetFile(String filePath)
      throws Exception {
    File dataFile = new File(URLDecoder.decode(getClass().getClassLoader().getResource(filePath).getFile(), "UTF-8"));
    ParquetNativeRecordReader recordReader = new ParquetNativeRecordReader();
    recordReader.init(dataFile, null, null);
    while (recordReader.hasNext()) {
      recordReader.next();
    }
    recordReader.rewind();
    while (recordReader.hasNext()) {
      recordReader.next();
    }

    recordReader.close();
  }
}
