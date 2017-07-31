/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.converter;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.writer.SingleColumnMultiValueWriter;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.metadata.column.ColumnMetadata;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;

public class SegmentFormatConverterV1ToV2 implements SegmentFormatConverter {

  private BufferedInputStream bis;
  private BufferedOutputStream bos;

  @Override
  public void convert(File indexSegmentDir)
      throws Exception {
    SegmentMetadataImpl segmentMetadataImpl = new SegmentMetadataImpl(indexSegmentDir);
    SegmentDirectory segmentDirectory =
        SegmentDirectory.createFromLocalFS(indexSegmentDir, segmentMetadataImpl, ReadMode.mmap);
    Set<String> columns = segmentMetadataImpl.getAllColumns();
    SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter();
    for (String column : columns) {
      ColumnMetadata columnMetadata = segmentMetadataImpl.getColumnMetadataFor(column);
      if (columnMetadata.isSorted()) {
        // no need to change sorted forward index
        continue;
      }
      PinotDataBuffer fwdIndexBuffer = segmentWriter.getIndexFor(column, ColumnIndexType.FORWARD_INDEX);
      if (columnMetadata.isSingleValue() && !columnMetadata.isSorted()) {

        // since we use dictionary to encode values, we wont have any negative values in forward
        // index
        boolean signed = false;

        SingleColumnSingleValueReader v1Reader =
            new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader(fwdIndexBuffer,
                segmentMetadataImpl.getTotalDocs(), columnMetadata.getBitsPerElement(), false);

        File convertedFwdIndexFile = new File(indexSegmentDir,
            column + V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION + ".tmp");
         SingleColumnSingleValueWriter v2Writer =
            new com.linkedin.pinot.core.io.writer.impl.v2.FixedBitSingleValueWriter(convertedFwdIndexFile,
                segmentMetadataImpl.getTotalDocs(), columnMetadata.getBitsPerElement());
        for (int row = 0; row < segmentMetadataImpl.getTotalDocs(); row++) {
          int value = v1Reader.getInt(row);
          v2Writer.setInt(row, value);
        }
        v1Reader.close();
        v2Writer.close();
        File fwdIndexFileCopy = new File(indexSegmentDir,
            column + V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION + ".orig");

        segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
        // FIXME
        PinotDataBuffer newIndexBuffer =
            segmentWriter.newIndexFor(column, ColumnIndexType.FORWARD_INDEX, (int) convertedFwdIndexFile.length());
        newIndexBuffer.readFrom(convertedFwdIndexFile);
        convertedFwdIndexFile.delete();
      }
      if (!columnMetadata.isSingleValue()) {

        // since we use dictionary to encode values, we wont have any negative values in forward
        // index
        boolean signed = false;

        SingleColumnMultiValueReader v1Reader = new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader(fwdIndexBuffer,
            segmentMetadataImpl.getTotalDocs(), columnMetadata.getTotalNumberOfEntries(),
            columnMetadata.getBitsPerElement(), signed);
        File convertedFwdIndexFile = new File(indexSegmentDir,
            column + V1Constants.Indexes.UN_SORTED_MV_FWD_IDX_FILE_EXTENTION + ".tmp");
        SingleColumnMultiValueWriter v2Writer = new com.linkedin.pinot.core.io.writer.impl.v2.FixedBitMultiValueWriter(convertedFwdIndexFile,
            segmentMetadataImpl.getTotalDocs(), columnMetadata.getTotalNumberOfEntries(),
            columnMetadata.getBitsPerElement());
        int[] values = new int[columnMetadata.getMaxNumberOfMultiValues()];
        for (int row = 0; row < segmentMetadataImpl.getTotalDocs(); row++) {
          int length = v1Reader.getIntArray(row, values);
          int[] copy  = new int[length];
          System.arraycopy(values, 0, copy, 0, length);
          v2Writer.setIntArray(row, copy);
        }
        v1Reader.close();
        v2Writer.close();
        segmentWriter.removeIndex(column, ColumnIndexType.FORWARD_INDEX);
        PinotDataBuffer newIndexBuffer = segmentWriter.newIndexFor(column, ColumnIndexType.FORWARD_INDEX,
            (int) convertedFwdIndexFile.length());
        newIndexBuffer.readFrom(convertedFwdIndexFile);
        convertedFwdIndexFile.delete();
      }
    }

    File metadataFile = new File(indexSegmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    File metadataFileCopy = new File(indexSegmentDir, V1Constants.MetadataKeys.METADATA_FILE_NAME + ".orig");
    bis = new BufferedInputStream(new FileInputStream(metadataFile));
    bos = new BufferedOutputStream(new FileOutputStream(metadataFileCopy));
    IOUtils.copy(bis, bos);
    bis.close();
    bos.close();

    final PropertiesConfiguration properties = new PropertiesConfiguration(metadataFileCopy);
    // update the segment version
    properties.setProperty(V1Constants.MetadataKeys.Segment.SEGMENT_VERSION,
        SegmentVersion.v2.toString());
    metadataFile.delete();
    properties.save(metadataFile);

  }

}
