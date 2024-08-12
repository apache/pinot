package org.apache.pinot.core.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Plan;


/**
 * A class to represent an abstract explain plan node.
 *
 * It is a generic POJO that is generated when {@link org.apache.pinot.core.query.request.context.ExplainMode#NODE}
 * explain is requested.
 */
public class ExplainInfo {
  private final String _type;
  private final Map<String, Plan.ExplainNode.AttributeValue> _attributes;
  private final List<ExplainInfo> _inputs;

  public ExplainInfo(String type) {
    _type = type;
    _attributes = Collections.emptyMap();
    _inputs = Collections.emptyList();
  }

  @JsonCreator
  public ExplainInfo(String type, Map<String, Plan.ExplainNode.AttributeValue> attributes, List<ExplainInfo> inputs) {
    _type = type;
    _attributes = attributes;
    _inputs = inputs;
  }

  public String getType() {
    return _type;
  }

  public Map<String, Plan.ExplainNode.AttributeValue> getAttributes() {
    return _attributes;
  }

  public List<ExplainInfo> getInputs() {
    return _inputs;
  }
}
