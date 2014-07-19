package com.linkedin.pinot.index.plan;

public abstract class Plan {

	abstract void print();

	/**
	 * Root node of the plan
	 * 
	 * @return
	 */
	abstract PlanNode getRoot();

	public void execute() {
		PlanNode root = getRoot();
		root.run();
	}
}
