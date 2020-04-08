package org.apache.pinot.plugin.inputformat.parquet;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/**
 * Extractor for Parquet records
 */
public class ParquetRecordExtractor implements RecordExtractor<GenericRecord> {

  @Override
  public GenericRow extract(List<String> sourceFieldNames, GenericRecord from, GenericRow to) {
    for (String fieldName : sourceFieldNames) {
      Object value = from.get(fieldName);
      Object convertedValue = RecordReaderUtils.convert(value);
      to.putValue(fieldName, convertedValue);
    }
    return to;
  }
}
