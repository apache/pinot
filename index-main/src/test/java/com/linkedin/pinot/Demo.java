package com.linkedin.pinot;

import com.linkedin.pinot.index.plan.FilterPlanNode;
import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.segment.IndexSegment;

public class Demo {
	public static void main(String[] args) {
		String queryString = "";
		//parse query string
		FilterQuery query = new FilterQuery();
		IndexSegment segment = null;
		FilterPlanNode filterPlan = new FilterPlanNode(segment, query);
		filterPlan.run();
	}
}
