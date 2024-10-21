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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.base.Preconditions;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


public class ProtoBufRecordReader implements RecordReader {
  private File _dataFile;
  private ProtoBufRecordExtractor _recordExtractor;

  private InputStream _inputStream;
  private boolean _hasNext;
  private Message.Builder _builder;

  private boolean hasMoreToRead()
      throws IOException {
    _inputStream.mark(1);
    int nextByte = _inputStream.read();
    _inputStream.reset();
    return nextByte != -1;
  }

  private void init()
      throws IOException {
    _inputStream = RecordReaderUtils.getBufferedInputStream(_dataFile);
    try {
      _hasNext = hasMoreToRead();
    } catch (Exception e) {
      _inputStream.close();
      throw e;
    }
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    ProtoBufRecordReaderConfig protoBufRecordReaderConfig = (ProtoBufRecordReaderConfig) recordReaderConfig;
    Preconditions.checkNotNull(protoBufRecordReaderConfig.getDescriptorFile(),
        "Protocol Buffer schema descriptor file must be provided");
    Descriptors.Descriptor descriptor = buildProtoBufDescriptor(protoBufRecordReaderConfig);
    _recordExtractor = new ProtoBufRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    DynamicMessage dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    _builder = dynamicMessage.newBuilderForType();
    init();
  }

  private Descriptors.Descriptor buildProtoBufDescriptor(ProtoBufRecordReaderConfig protoBufRecordReaderConfig)
      throws IOException {
    try {
      InputStream fin = ProtoBufUtils.getDescriptorFileInputStream(
          protoBufRecordReaderConfig.getDescriptorFile().toString());
      DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
      Descriptors.FileDescriptor fileDescriptor =
          Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[]{});
      return fileDescriptor.getMessageTypes().get(0);
    } catch (Exception e) {
      throw new IOException("Failed to create Protobuf descriptor", e);
    }
  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    Message message;
    try {
      _builder.mergeDelimitedFrom(_inputStream);
      message = _builder.build();
    } catch (Exception e) {
      throw new IOException("Caught exception while reading protobuf object", e);
    } finally {
      _builder.clear();
    }
    _recordExtractor.extract(message, reuse);
    _hasNext = hasMoreToRead();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _inputStream.close();
    init();
  }

  @Override
  public void close()
      throws IOException {
    _inputStream.close();
  }
}
