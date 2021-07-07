package org.apache.pinot.segment.spi.creator.name;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class InputFileSegmentNameGenerator implements SegmentNameGenerator {

    private static final String PARAMETER_TEMPLATE = "${filePathPattern:\\%d}";
    
    private Pattern filePathPattern;
    private String segmentNameTemplate;
    
    public InputFileSegmentNameGenerator(String filePathPattern, String segmentNameTemplate) {
        this.filePathPattern = Pattern.compile(filePathPattern);
        this.segmentNameTemplate = segmentNameTemplate;
    }
    
    @Override
    public String generateSegmentName(int sequenceId, Object minTimeValue, Object maxTimeValue,
            String inputFilePath) {
        
        if (inputFilePath == null) {
            if (sequenceId >= 0) {
                return String.format("InvalidInputFilePath_%d", sequenceId);
            } else {
                return "InvalidInputFilePath";
            }
        }
        
        // TODO use URI for inputFilePath, so we can strip off the protocol?
        Matcher m = filePathPattern.matcher(inputFilePath);
        if (!m.matches()) {
            //TODO log warning
            while (inputFilePath.startsWith("/")) {
                inputFilePath = inputFilePath.substring(1);
            }
            
            return inputFilePath.replaceAll("[/\\.]", "_");
        }
        
        String segmentName = segmentNameTemplate;
        for (int i = 1; i <= m.groupCount(); i++) {
            segmentName = segmentName.replace(String.format(PARAMETER_TEMPLATE, i), m.group(i));
        }
        
        return segmentName;
    }
    
    @Override
    public String toString() {
        return String.format(
                "InputFileSegmentNameGenerator: filePathPattern=%s, segmentNameTemplate=%s",
                filePathPattern, segmentNameTemplate);
    }
}
