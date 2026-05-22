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
package org.apache.pinot.tools.segment.converter;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.pinot.core.util.SegmentProcessorAvroUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.plugin.inputformat.parquet.ParquetUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The <code>PinotSegmentToParquetConverter</code> class is the tool to convert Pinot segment to Parquet format.
 */
public class PinotSegmentToParquetConverter implements PinotSegmentConverter {
  private final String _segmentDir;
  private final String _outputFile;
  private final CompressionCodecName _compressionCodec;
  private final boolean _forwardIndexOnly;

  public PinotSegmentToParquetConverter(String segmentDir, String outputFile) {
    this(segmentDir, outputFile, CompressionCodecName.GZIP, false);
  }

  public PinotSegmentToParquetConverter(String segmentDir, String outputFile,
      CompressionCodecName compressionCodec, boolean forwardIndexOnly) {
    _segmentDir = segmentDir;
    _outputFile = outputFile;
    _compressionCodec = compressionCodec;
    _forwardIndexOnly = forwardIndexOnly;
  }

  @Override
  public void convert()
      throws Exception {
    File indexDir = new File(_segmentDir);
    Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(new SegmentMetadataImpl(indexDir).getSchema());
    Configuration hadoopConf = ParquetUtils.getParquetHadoopConfiguration();
    OutputFile outputFile = HadoopOutputFile.fromPath(new Path(_outputFile), hadoopConf);
    PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader();
    pinotSegmentRecordReader.init(new File(_segmentDir), null, null, false, _forwardIndexOnly);
    try (pinotSegmentRecordReader) {
      try (ParquetWriter<Record> parquetWriter =
          AvroParquetWriter.<Record>builder(outputFile).withSchema(avroSchema)
              .withCompressionCodec(_compressionCodec)
              .withConf(hadoopConf).build()) {
        GenericRow row = new GenericRow();
        Record reusableRecord = new Record(avroSchema);
        while (pinotSegmentRecordReader.hasNext()) {
          row = pinotSegmentRecordReader.next(row);
          Record record = SegmentProcessorAvroUtils.convertGenericRowToAvroRecord(row, reusableRecord);
          parquetWriter.write(record);
          row.clear();
        }
      }
    }
  }
}
