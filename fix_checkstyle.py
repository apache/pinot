#!/usr/bin/env python3
import re
import os
import glob

def fix_line_length(content):
    """Fix lines longer than 120 characters by breaking them appropriately"""
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        if len(line) <= 120:
            fixed_lines.append(line)
            continue
            
        # Handle long method calls
        if 'executeQueryWithCursor(' in line and len(line) > 120:
            # Break after the first parameter
            line = re.sub(r'executeQueryWithCursor\("([^"]+)",\s*"([^"]+)",\s*(\d+)\)', 
                         r'executeQueryWithCursor("\1",\n        "\2", \3)', line)
        
        # Handle long ResultCursorImpl constructor calls
        if 'new ResultCursorImpl(' in line and len(line) > 120:
            line = re.sub(r'new ResultCursorImpl\(([^,]+),\s*([^,]+),\s*([^,]+),\s*(false|true)\)', 
                         r'new ResultCursorImpl(\1, \2, \3,\n        \4)', line)
        
        # Handle long seekToPageAsync calls
        if 'seekToPageAsync(' in line and len(line) > 120:
            line = re.sub(r'seekToPageAsync\(([^,]+),\s*([^,]+),\s*([^)]+)\)', 
                         r'seekToPageAsync(\1, \2,\n        \3)', line)
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def remove_trailing_whitespace(content):
    """Remove trailing whitespace from all lines"""
    lines = content.split('\n')
    return '\n'.join(line.rstrip() for line in lines)

def fix_blank_lines(content):
    """Fix blank line issues"""
    # Remove blank line before closing brace
    content = re.sub(r'\n\s*\n\s*}', r'\n  }', content)
    return content

def process_java_file(file_path):
    """Process a single Java file to fix checkstyle issues"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Apply fixes
    content = remove_trailing_whitespace(content)
    content = fix_line_length(content)
    content = fix_blank_lines(content)
    
    with open(file_path, 'w') as f:
        f.write(content)

def main():
    # Find all Java test files
    test_files = glob.glob('pinot-clients/pinot-java-client/src/test/java/**/*.java', recursive=True)
    
    for file_path in test_files:
        print(f"Processing {file_path}")
        process_java_file(file_path)
    
    print("Checkstyle fixes applied!")

if __name__ == '__main__':
    main()
