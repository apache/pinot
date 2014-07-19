package com.linkedin.pinot.index.common;

public class BlockId {
	
	public BlockId(int id) {
		this.id = id;
	}

	int id;
	
	public int getId(){
		return id;
	}

}
