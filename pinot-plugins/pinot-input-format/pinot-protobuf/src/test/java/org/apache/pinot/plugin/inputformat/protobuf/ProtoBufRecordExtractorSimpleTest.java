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
