package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;


public class ExtractTransformFunction extends BaseTransformFunction {
  public static final String _name = "extract";
  private TransformResultMetadata _resultMetadata;
  private String[] _stringOutputTimes;
  protected String _field;
  protected String _exp;
  
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
    _exp = ((LiteralTransformFunction) arguments.get(1)).getLiteral();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();

    if (_stringOutputTimes == null || _stringOutputTimes.length < numDocs) {
      _stringOutputTimes = new String[numDocs];
    }

    
    
//    convert(times, numDocs, _stringValuesSV);

    return _stringValuesSV;
  }

  private void convert(String[] times, int numDocs, String[] stringValuesSV) {
    for(int i=0; i<numDocs; ++i) {
      System.out.println(times[i]);
    }
  }
}
