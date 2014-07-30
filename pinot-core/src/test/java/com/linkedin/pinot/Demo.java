package com.linkedin.pinot;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.query.FilterQuery;

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
