package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;


public class ExtractTransformFunction extends BaseTransformFunction {
  public static final String _name = "extract";
  private TransformResultMetadata _resultMetadata;
  private TransformFunction _mainTranformFunction;
  private String[] _stringOutputTimes;
  protected String _field;
  protected Chronology _chronology = ISOChronology.getInstanceUTC();
  
  private static final TransformResultMetadata METADATA =
      new TransformResultMetadata(FieldSpec.DataType.INT, true, false);
  
  @Override
  public String getName() { return _name; }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    if (arguments.size() != 2) { 
      throw new IllegalArgumentException("Exactly 2 arguments are required for EXTRACT transform function"); 
    }

    _field = ((LiteralTransformFunction) arguments.get(0)).getLiteral();
    _mainTranformFunction = arguments.get(1);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();

    if (_stringValuesSV == null || _stringValuesSV.length < numDocs) {
      _stringValuesSV = new String[numDocs];
    }

    long[] timestamps = _mainTranformFunction.transformToLongValuesSV(projectionBlock);
    
    convert(timestamps, numDocs, _stringValuesSV);

    return _stringValuesSV;
  }

  private void convert(long[] timestamps, int numDocs, String[] output) {
    for(int i=0; i<numDocs; ++i) {
      DateTimeField accessor;
      
      switch (_field) {
        case "YEAR":
          accessor = _chronology.year();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        case "MONTH":
          accessor = _chronology.monthOfYear();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        case "DAY":
          accessor = _chronology.dayOfMonth();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        case "HOUR":
          accessor = _chronology.hourOfDay();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        case "MINUTE":
          accessor = _chronology.minuteOfHour();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        case "SECOND":
          accessor = _chronology.secondOfMinute();
          output[i] = String.valueOf(accessor.get(timestamps[i]));
          break;
        default:
      }
    }
  }
}
