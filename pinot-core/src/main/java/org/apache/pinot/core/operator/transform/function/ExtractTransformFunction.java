package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public abstract class ExtractTransformFunction extends BaseTransformFunction {
  private static final TransformResultMetadata METADATA =
      new TransformResultMetadata(FieldSpec.DataType.INT, true, false);
  private final String _name;
  private TransformFunction _timestampsFunction;
  protected Chronology _chronology;
  protected static final Chronology UTC = ISOChronology.getInstanceUTC();

  protected ExtractTransformFunction(String name) { _name = name; }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(!arguments.isEmpty() && arguments.size() <= 2, "%s takes one or two arguments", _name);
    _timestampsFunction = arguments.get(0);
    if (arguments.size() == 2) {
      Preconditions.checkArgument(arguments.get(1) instanceof LiteralTransformFunction,
          "zoneId parameter %s must be a literal", _name);
      _chronology =
          ISOChronology.getInstance(DateTimeZone.forID(((LiteralTransformFunction) arguments.get(1)).getLiteral()));
    } else {
      _chronology = UTC;
    }
  }

  @Override
  public String getName() { return _name; }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return METADATA;
  }

  protected abstract void convert(String timestamps, String output);

  public static final class Year extends ExtractTransformFunction {
    public Year() { super(TransformFunctionType.YEAR.getName()); }

    @Override
    protected void convert(String timestamp, String output) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");
      DateTime dateTime = formatter.parseDateTime(timestamp);
      output = String.valueOf(dateTime.year());
    }
  }

  public static final class Minute extends ExtractTransformFunction {
    public Minute() { super(TransformFunctionType.MINUTE.getName()); }

    @Override
    protected void convert(String timestamp, String output) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");
      DateTime dateTime = formatter.parseDateTime(timestamp);
      output = String.valueOf(dateTime.minuteOfHour());
    }
  }
}
