package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;


public class ExtractTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "extract";
  private TransformFunction _mainTransformFunction;
  protected Field _field;
  protected Chronology _chronology = ISOChronology.getInstanceUTC();

  private enum Field {
    YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for EXTRACT transform function");
    }

    _field = Field.valueOf(((LiteralTransformFunction) arguments.get(0)).getLiteral());

    _mainTransformFunction = arguments.get(1);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();

    if (_intValuesSV == null || _intValuesSV.length < numDocs) {
      _intValuesSV = new int[numDocs];
    }

    long[] timestamps = _mainTransformFunction.transformToLongValuesSV(projectionBlock);

    convert(timestamps, numDocs, _intValuesSV);

    return _intValuesSV;
  }

  private void convert(long[] timestamps, int numDocs, int[] output) {
    for (int i = 0; i < numDocs; ++i) {
      DateTimeField accessor;
      
      switch (_field) {
        case YEAR:
          accessor = _chronology.year();
          output[i] = accessor.get(timestamps[i]);
          break;
        case MONTH:
          accessor = _chronology.monthOfYear();
          output[i] = accessor.get(timestamps[i]);
          break;
        case DAY:
          accessor = _chronology.dayOfMonth();
          output[i] = accessor.get(timestamps[i]);
          break;
        case HOUR:
          accessor = _chronology.hourOfDay();
          output[i] = accessor.get(timestamps[i]);
          break;
        case MINUTE:
          accessor = _chronology.minuteOfHour();
          output[i] = accessor.get(timestamps[i]);
          break;
        case SECOND:
          accessor = _chronology.secondOfMinute();
          output[i] = accessor.get(timestamps[i]);
          break;
        default:
          throw new IllegalArgumentException("Unsupported FIELD type");
      }
    }
  }
}
