/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.routing.segmentpruner;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.FilterKind;


public class SegmentNameSegmentPruner implements SegmentPruner {

  @Override
  public void init(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
  }

  @Override
  public void onAssignmentChange(IdealState idealState, ExternalView externalView, Set<String> onlineSegments) {
  }

  @Override
  public void refreshSegment(String segment) {
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null || !isEligibleForPruning(filterExpression)) {
      return segments;
    }

    return getSegmentsEligibleSegments(filterExpression, segments);
  }

  private boolean isEligibleForPruning(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();

    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (isEligibleForPruning(child)) {
            return true;
          }
        }
        break;

      case OR:
        for (Expression child : operands) {
          if (!isEligibleForPruning(child)) {
            return false;
          }
        }
        break;

        // TODO we can also add LIKE, REGEX_LIKE, TEXT_CONTAINS, TEXT_MATCH etc
      case NOT_EQUALS:
      case EQUALS:
      case IN:
      case NOT_IN:
        Identifier identifier = operands.get(0).getIdentifier();
        return identifier != null && identifier.getName()
            .equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME);
      default:
        return false;
    }

    return false;
  }

  private Set<String> getSegmentsEligibleSegments(Expression filterExpression, Set<String> segments) {
    Set<String> result = new HashSet<>(segments);
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();

    Identifier identifier;

    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          result.retainAll(getSegmentsEligibleSegments(child, segments));
        }
        break;

      case OR:
        result = new HashSet<>();
        for (Expression child : operands) {
          result.addAll(getSegmentsEligibleSegments(child, segments));
        }
        break;

      case EQUALS:
        identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName()
            .equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
          result.retainAll(Collections.singleton(operands.get(1).getLiteral().getFieldValue().toString()));
        }
        break;

      case NOT_EQUALS:
        identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName()
            .equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
          result.removeAll(Collections.singleton(operands.get(1).getLiteral().getFieldValue().toString()));
        }
        break;

      case IN:
        identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName()
            .equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
          int numOperands = operands.size();
          Set<String> segmentNames = new HashSet<>();
          for (int i = 1; i < numOperands; i++) {
            segmentNames.add(operands.get(i).getLiteral().getFieldValue().toString());
          }
          result.retainAll(segmentNames);
        }
        break;
      case NOT_IN:
        identifier = operands.get(0).getIdentifier();
        if (identifier != null && identifier.getName()
            .equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
          int numOperands = operands.size();
          Set<String> segmentNames = new HashSet<>();
          for (int i = 1; i < numOperands; i++) {
            segmentNames.add(operands.get(i).getLiteral().getFieldValue().toString());
          }
          result.removeAll(segmentNames);
        }
        break;
      default:
        break;
    }

    return result;
  }
}
