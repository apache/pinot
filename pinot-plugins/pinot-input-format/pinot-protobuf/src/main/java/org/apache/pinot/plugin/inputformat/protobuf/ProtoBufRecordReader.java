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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;
import org.apache.pinot.spi.utils.ResourceFinder;


public class ProtoBufRecordReader implements RecordReader {
  private File _dataFile;
  private ProtoBufRecordExtractor _recordExtractor;

  private InputStream _inputStream;
  private boolean _hasNext;
  private DynamicMessage _dynamicMessage;

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
    InputStream fin = getDescriptorFileInputStream(protoBufRecordReaderConfig);
    Descriptors.Descriptor descriptor = buildProtoBufDescriptor(fin);
    _recordExtractor = new ProtoBufRecordExtractor();
    _recordExtractor.init(fieldsToRead, null);
    _dynamicMessage = DynamicMessage.getDefaultInstance(descriptor);
    init();
  }

  private Descriptors.Descriptor buildProtoBufDescriptor(InputStream fin)
      throws IOException {
    try {
      DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(fin);
      Descriptors.FileDescriptor fileDescriptor =
          Descriptors.FileDescriptor.buildFrom(set.getFile(0), new Descriptors.FileDescriptor[]{});
      return fileDescriptor.getMessageTypes().get(0);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IOException("Descriptor file validation failed", e);
    }
  }

  private InputStream getDescriptorFileInputStream(ProtoBufRecordReaderConfig protoBufRecordReaderConfig)
      throws IOException {
    URI descriptorFileURI = protoBufRecordReaderConfig.getDescriptorFile();
    return ResourceFinder.openResource(descriptorFileURI);
  }

  @Override
  public boolean hasNext() {
    return _hasNext;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    Message message;
    try {
      Message.Builder builder = _dynamicMessage.newBuilderForType();
      builder.mergeDelimitedFrom(_inputStream);
      message = builder.build();
    } catch (Exception e) {
      throw new IOException("Caught exception while reading protobuf object", e);
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
