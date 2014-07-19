package com.linkedin.pinot.index.plan;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.pinot.index.common.Operator;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.operator.BAndOperator;
import com.linkedin.pinot.index.operator.BOrOperator;
import com.linkedin.pinot.index.operator.DataSource;
import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.index.query.FilterQuery.FilterOperator;
import com.linkedin.pinot.index.segment.DataSourceProvider;
import com.linkedin.pinot.index.segment.IndexSegment;
import static com.linkedin.pinot.index.common.Predicate.Type.*;

public class FilterPlanNode implements PlanNode {

	private final FilterQuery filterQuery;
	private final IndexSegment segment;

	public FilterPlanNode(IndexSegment segment, FilterQuery filterQuery) {
		this.segment = segment;
		this.filterQuery = filterQuery;
	}

	@Override
	public Operator run() {
		return constructPhysicalOperator(filterQuery);
	}

	private Operator constructPhysicalOperator(FilterQuery filter) {
		Operator ret = null;
		List<FilterQuery> childFilters = filter.getNestedFilterConditions();
		boolean isLeaf = childFilters == null || childFilters.isEmpty();
		List<Operator> childOperators = null;
		if (!isLeaf) {
			childOperators = new ArrayList<Operator>();
			for (FilterQuery query : childFilters) {
				childOperators.add(constructPhysicalOperator(query));
			}
			FilterOperator filterType = filter.getOperator();
			switch (filterType) {
			case AND:
				ret = new BAndOperator(childOperators);
				break;
			case OR:
				ret = new BOrOperator(childOperators);
				break;
			}
		} else {
			FilterOperator filterType = filter.getOperator();
			String column = filter.getColumn();
			Predicate predicate = null;
			List<String> value = filter.getValue();
			switch (filterType) {
			case EQUALITY:
				predicate = new Predicate(column, EQ, value);
				break;
			case RANGE:
				predicate = new Predicate(column, RANGE, value);
				break;
			case REGEX:
				predicate = new Predicate(column, REGEX, value);
				break;
			case NOT:
				predicate = new Predicate(column, NEQ, value);
				break;
			}
			DataSource ds;
			if (predicate != null) {
				ds = segment.getDataSource(column, predicate);
			} else {
				ds = segment.getDataSource(column);
			}
			ret = ds;
		}
		return ret;
	}

	@Override
	public void print() {

	}
}
