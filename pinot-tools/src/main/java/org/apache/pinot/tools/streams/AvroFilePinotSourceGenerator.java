package org.apache.pinot.tools.streams;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroFilePinotSourceGenerator implements PinotSourceGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSourceStream.class);
  private DataFileStream<GenericRecord> _avroDataStream;
  private List<byte[]> _reusedBuffer;
  private Schema _pinotSchema;
  private long _rowsProduced;
  private String _timeColumnName;
  private Function<Long, Long> _rowNumberToTimeIndex;
  private URI _avroFile;
  public AvroFilePinotSourceGenerator(Schema pinotSchema, URI avroFile, String timeColumnName, Function<Long, Long> rowNumberToTimeIndex) {
    _pinotSchema = pinotSchema;
    DateTimeFieldSpec timeColumnSpec = pinotSchema.getSpecForTimeColumn(timeColumnName);
    Preconditions.checkNotNull(timeColumnSpec, "Time column " + timeColumnName + " is not found in schema, or is not a valid DateTime column");
    _rowsProduced = 0;
    _rowNumberToTimeIndex = rowNumberToTimeIndex;
    _timeColumnName = timeColumnName;
    _reusedBuffer = new ArrayList<>();
    _avroFile = avroFile;
  }
  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<byte[]> generateRows() {
    _reusedBuffer.clear();
    ensureStream();
    if (_avroDataStream.hasNext()) {
      GenericRecord record = _avroDataStream.next();
      GenericRecord message = new GenericData.Record(AvroUtils.getAvroSchemaFromPinotSchema(_pinotSchema));
      for (FieldSpec spec : _pinotSchema.getDimensionFieldSpecs()) {
        message.put(spec.getName(), record.get(spec.getName()));
      }

      for (FieldSpec spec : _pinotSchema.getMetricFieldSpecs()) {
        message.put(spec.getName(), record.get(spec.getName()));
      }
      message.put(_timeColumnName, _rowNumberToTimeIndex.apply(_rowsProduced));
      _reusedBuffer.add(message.toString().getBytes(StandardCharsets.UTF_8));
      _rowsProduced += 1;
    }
    return _reusedBuffer;
  }

  @Override
  public void close()
      throws Exception {
    _avroDataStream.close();
  }

  private void ensureStream() {
    try {
      if (_avroDataStream == null || !_avroDataStream.hasNext()) {
        _avroDataStream = new DataFileStream<>(new FileInputStream(new File(_avroFile.getPath())), new GenericDatumReader<>());
      }
    } catch (IOException ex) {
      LOGGER.error("Failed to open {}", _avroFile.getPath(), ex);
      throw new RuntimeException("Failed to open " + _avroFile.getPath(), ex);
    }
  }
}
