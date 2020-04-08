package org.apache.pinot.plugin.inputformat.thrift;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;
import org.apache.thrift.TBase;


/**
 * Extractor for records of Thrift input
 */
public class ThriftRecordExtractor implements RecordExtractor<TBase> {

  private Map<String, Integer> _fieldIds;

  @Override
  public void init(RecordExtractorConfig recordExtractorConfig) {
    _fieldIds = ((ThriftRecordExtractorConfig) recordExtractorConfig).getFieldIds();
  }

  @Override
  public GenericRow extract(List<String> sourceFieldNames, TBase from, GenericRow to) {
    for (String fieldName : sourceFieldNames) {
      Object value = null;
      Integer fieldId = _fieldIds.get(fieldName);
      if (fieldId != null) {
        //noinspection unchecked
        value = from.getFieldValue(from.fieldForId(fieldId));
      }
      Object convertedValue = RecordReaderUtils.convert(value);
      to.putValue(fieldName, convertedValue);
    }
    return to;
  }
}
