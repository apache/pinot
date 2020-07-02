import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/**
 * Record reader for AVRO file.
 */
public class TestRecordReader implements RecordReader {

  List<GenericRow> _rows = new ArrayList<>();
  Iterator<GenericRow> _iterator;

  public void init(File dataFile, Set<String> fields, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    int numRows = 10;
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue("key", "value-" + i);
      _rows.add(row);
    }
    _iterator = _rows.iterator();
  }

  public boolean hasNext() {
    return _iterator.hasNext();
  }

  public GenericRow next()
      throws IOException {
    return _iterator.next();
  }

  public GenericRow next(GenericRow reuse)
      throws IOException {
    return _iterator.next();
  }

  public void rewind()
      throws IOException {
    _iterator = _rows.iterator();
  }

  public void close() {

  }
}