package org.apache.pinot.segment.local.segment.creator.impl;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;

public class ColumnJsonParserException extends JsonParseException {
    /**
     * Exception type for parsing problems when
     * processing JSON content in a column
     * Sub-class of {@link com.fasterxml.jackson.core.JsonParseException}.
     */
    final static long serialVersionUID = 123; // Stupid eclipse...

    final String columnName;

    protected ColumnJsonParserException(String columnName, String msg, JsonLocation loc, Throwable rootCause)
    {
        /* Argh. IOException(Throwable,String) is only available starting
         * with JDK 1.6...
         */
        super(msg, loc, rootCause);
        this.columnName = columnName;
    }

    /**
     * Default method overridden so that we can add location information
     */
    @Override
    public String getMessage()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Column: ");
        sb.append(this.columnName);
        sb.append("\n");
        sb.append(super.getMessage());
        return sb.toString();
    }

    @Override
    public String toString() {
        return getClass().getName()+": "+getMessage();
    }

}
