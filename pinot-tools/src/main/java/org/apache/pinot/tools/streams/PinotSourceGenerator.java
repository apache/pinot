package org.apache.pinot.tools.streams;

import java.util.List;
import java.util.Properties;


public interface PinotSourceGenerator extends AutoCloseable {
  void init(Properties properties);
  List<byte[]> generateRows();
}
