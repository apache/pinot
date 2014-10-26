package com.linkedin.pinot.core.index.reader;

public interface DataFileReader {

	/**
	 * opens the reader. None of the access methods can be called if open is not
	 * invoked
	 */

	boolean open();

	/**
	 * Provides the metadata about the data file
	 * @return
	 */
	DataFileMetadata getMetadata();

	/**
	 * 
	 * @return
	 */
	boolean close();

}
