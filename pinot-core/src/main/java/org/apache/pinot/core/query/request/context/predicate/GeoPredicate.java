package org.apache.pinot.core.query.request.context.predicate;

import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.locationtech.jts.geom.Geometry;


//TODO: Make this flexible
public class GeoPredicate {

  //this is the column name
  ExpressionContext _lhs;

  Type type;

  Geometry _geometry;

  double _distance;

  enum Type {
    WITHIN, OVERLAP;
  }
}
