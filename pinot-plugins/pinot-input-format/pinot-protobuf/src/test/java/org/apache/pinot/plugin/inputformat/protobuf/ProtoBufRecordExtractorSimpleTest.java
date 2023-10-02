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
import java.util.Locale;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class ProtoBufRecordExtractorSimpleTest {

  private Descriptors.FileDescriptor _fileDescriptor;
  private Descriptors.Descriptor _messageDescriptor;
  private Descriptors.FieldDescriptor _optionalDescriptor;
  private Descriptors.FieldDescriptor _nonOptionalDescriptor;

  private ProtoBufRecordExtractor _protoBufRecordExtractor;
  @BeforeTest
  public void init()
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.FieldDescriptorProto myOptionalInt =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("myOptionalInt")
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32)
            .setNumber(1)
            .setProto3Optional(true)
            .build();
    DescriptorProtos.FieldDescriptorProto myNonOptionalInt =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("myNonOptionalInt")
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32)
            .setNumber(2)
            .setProto3Optional(false)
            .build();
    DescriptorProtos.FileDescriptorProto build = DescriptorProtos.FileDescriptorProto.newBuilder()
        .setName("withOptional")
        .setSyntax(Descriptors.FileDescriptor.Syntax.PROTO3.name().toLowerCase(Locale.US))
        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
            .setName("myMessage")
            .addField(myOptionalInt)
            .addField(myNonOptionalInt)
        ).build();

    _fileDescriptor = Descriptors.FileDescriptor.buildFrom(build, new Descriptors.FileDescriptor[] {});
    _fileDescriptor.getSyntax();
    _messageDescriptor = _fileDescriptor.findMessageTypeByName("myMessage");
    _optionalDescriptor = _messageDescriptor.findFieldByName("myOptionalInt");
    _nonOptionalDescriptor = _messageDescriptor.findFieldByName("myNonOptionalInt");

    _protoBufRecordExtractor = new ProtoBufRecordExtractor();
    _protoBufRecordExtractor.init(null, new RecordExtractorConfig() {
    });
  }

  @Test
  public void whenOptionalAndPresent() {
    Message message = DynamicMessage.newBuilder(_messageDescriptor)
        .setField(_optionalDescriptor, 0)
        .build();

    GenericRow row = new GenericRow();
    _protoBufRecordExtractor.extract(message, row);

    Assert.assertEquals(row.getValue(_optionalDescriptor.getName()), 0);
  }

  @Test
  public void whenOptionalAndAbsent() {
    Message message = DynamicMessage.newBuilder(_messageDescriptor)
        .clearField(_optionalDescriptor)
        .build();

    GenericRow row = new GenericRow();
    _protoBufRecordExtractor.extract(message, row);

    Assert.assertNull(row.getValue(_optionalDescriptor.getName()));
  }

  @Test
  public void whenNotOptionalAndPresent() {
    Message message = DynamicMessage.newBuilder(_messageDescriptor)
        .setField(_nonOptionalDescriptor, 0)
        .build();

    GenericRow row = new GenericRow();
    _protoBufRecordExtractor.extract(message, row);

    Assert.assertEquals(row.getValue(_nonOptionalDescriptor.getName()), 0);
  }

  @Test
  public void whenNotOptionalAndAbsent() {
    Message message = DynamicMessage.newBuilder(_messageDescriptor)
        .clearField(_nonOptionalDescriptor)
        .build();

    GenericRow row = new GenericRow();
    _protoBufRecordExtractor.extract(message, row);

    Assert.assertEquals(row.getValue(_nonOptionalDescriptor.getName()), null);
  }
}
