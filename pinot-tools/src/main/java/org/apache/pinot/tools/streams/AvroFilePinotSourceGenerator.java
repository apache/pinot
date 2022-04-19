package org.apache.pinot.tools.streams;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.Strings;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroFilePinotSourceGenerator implements PinotSourceGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeSource.class);
  private DataFileStream<GenericRecord> _avroDataStream;
  private final List<byte[]> _reusedBuffer;
  private final Schema _pinotSchema;
  private long _rowsProduced;
  private final String _timeColumnName;
  private final Function<Long, Long> _rowNumberToTimeIndex;
  private final File _avroFile;
  private final int _rowsPerBatch;

  public AvroFilePinotSourceGenerator(Schema pinotSchema, File avroFile) {
    this(pinotSchema, avroFile, 1,  null, null);
  }

  public AvroFilePinotSourceGenerator(Schema pinotSchema, File avroFile, int rowsPerBatch, @Nullable String timeColumnName,
      @Nullable Function<Long, Long> rowNumberToTimeIndex) {
    _pinotSchema = pinotSchema;
    _rowsProduced = 0;
    _rowNumberToTimeIndex = rowNumberToTimeIndex;
    _timeColumnName = timeColumnName;
    if (!Strings.isNullOrEmpty(_timeColumnName)) {
      DateTimeFieldSpec timeColumnSpec = pinotSchema.getSpecForTimeColumn(timeColumnName);
      Preconditions.checkNotNull(timeColumnSpec,
          "Time column " + timeColumnName + " is not found in schema, or is not a valid DateTime column");
    }
    _reusedBuffer = new ArrayList<>();
    _avroFile = avroFile;
    _rowsPerBatch = rowsPerBatch;
  }

  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<byte[]> generateRows() {
    _reusedBuffer.clear();
    ensureStream();
    int rowsInCurrentBatch = 0;
    while (_avroDataStream.hasNext() && rowsInCurrentBatch < _rowsPerBatch) {
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
      rowsInCurrentBatch += 1;
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
      if (_avroDataStream != null && !_avroDataStream.hasNext()) {
        _avroDataStream.close();
        _avroDataStream = null;
      }
      if (_avroDataStream == null) {
        _avroDataStream = new DataFileStream<>(new FileInputStream(_avroFile.getPath()), new GenericDatumReader<>());
      }
    } catch (IOException ex) {
      LOGGER.error("Failed to open/close {}", _avroFile.getPath(), ex);
      throw new RuntimeException("Failed to open/close " + _avroFile.getPath(), ex);
    }
  }
}
