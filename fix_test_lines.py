#!/usr/bin/env python3
import re

def fix_prepared_statement_test():
    """Fix line length violations in PreparedStatementTest.java"""
    file_path = 'pinot-clients/pinot-java-client/src/test/java/org/apache/pinot/client/PreparedStatementTest.java'
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix long lines by breaking them appropriately
    content = re.sub(
        r'Assert\.assertEquals\(preparedStatement\.getParameterMetaData\(\)\.getParameterCount\(\), 2\);',
        'Assert.assertEquals(preparedStatement.getParameterMetaData().getParameterCount(),\n        2);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(preparedStatement\.getParameterMetaData\(\)\.getParameterType\(1\), Types\.VARCHAR\);',
        'Assert.assertEquals(preparedStatement.getParameterMetaData().getParameterType(1),\n        Types.VARCHAR);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(preparedStatement\.getParameterMetaData\(\)\.getParameterCount\(\), 3\);',
        'Assert.assertEquals(preparedStatement.getParameterMetaData().getParameterCount(),\n        3);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(preparedStatement\.getParameterMetaData\(\)\.getParameterTypeName\(1\), "VARCHAR"\);',
        'Assert.assertEquals(preparedStatement.getParameterMetaData().getParameterTypeName(1),\n        "VARCHAR");',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)

def fix_result_set_group_test():
    """Fix line length violations in ResultSetGroupTest.java"""
    file_path = 'pinot-clients/pinot-java-client/src/test/java/org/apache/pinot/client/ResultSetGroupTest.java'
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix long lines by breaking them appropriately
    content = re.sub(
        r'Assert\.assertEquals\(resultSetGroup\.getResultSet\(0\)\.getMetaData\(\)\.getColumnCount\(\), 2\);',
        'Assert.assertEquals(resultSetGroup.getResultSet(0).getMetaData().getColumnCount(),\n        2);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(resultSetGroup\.getResultSet\(0\)\.getMetaData\(\)\.getColumnType\(1\), Types\.VARCHAR\);',
        'Assert.assertEquals(resultSetGroup.getResultSet(0).getMetaData().getColumnType(1),\n        Types.VARCHAR);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(resultSetGroup\.getResultSet\(0\)\.getMetaData\(\)\.getColumnCount\(\), 3\);',
        'Assert.assertEquals(resultSetGroup.getResultSet(0).getMetaData().getColumnCount(),\n        3);',
        content
    )
    
    content = re.sub(
        r'Assert\.assertEquals\(resultSetGroup\.getResultSet\(0\)\.getMetaData\(\)\.getColumnTypeName\(1\), "VARCHAR"\);',
        'Assert.assertEquals(resultSetGroup.getResultSet(0).getMetaData().getColumnTypeName(1),\n        "VARCHAR");',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)

if __name__ == '__main__':
    fix_prepared_statement_test()
    fix_result_set_group_test()
    print("Fixed remaining test file line length violations!")
