#!/usr/bin/env python3
import re

def fix_prepared_statement_test():
    """Fix specific line length violations in PreparedStatementTest.java"""
    file_path = 'pinot-clients/pinot-java-client/src/test/java/org/apache/pinot/client/PreparedStatementTest.java'
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix line 115
    content = re.sub(
        r'      return CompletableFuture\.completedFuture\(CursorAwareBrokerResponse\.fromBrokerResponse\(BrokerResponse\.empty\(\), null, 0, 0\)\);',
        '      return CompletableFuture.completedFuture(\n          CursorAwareBrokerResponse.fromBrokerResponse(BrokerResponse.empty(), null, 0, 0));',
        content
    )
    
    # Fix line 126 (similar pattern)
    content = re.sub(
        r'      return CompletableFuture\.completedFuture\(\n          CursorAwareBrokerResponse\.fromBrokerResponse\(BrokerResponse\.empty\(\), null, 0, 0\)\);',
        '      return CompletableFuture.completedFuture(\n          CursorAwareBrokerResponse.fromBrokerResponse(BrokerResponse.empty(), null, 0, 0));',
        content
    )
    
    # Fix line 138 (similar pattern)
    content = re.sub(
        r'      return CompletableFuture\.completedFuture\(CursorAwareBrokerResponse\.fromBrokerResponse\(BrokerResponse\.empty\(\), null, 0, 0\)\);',
        '      return CompletableFuture.completedFuture(\n          CursorAwareBrokerResponse.fromBrokerResponse(BrokerResponse.empty(), null, 0, 0));',
        content
    )
    
    with open(file_path, 'w') as f:
        f.write(content)

def fix_result_set_group_test():
    """Fix specific line length violations in ResultSetGroupTest.java"""
    file_path = 'pinot-clients/pinot-java-client/src/test/java/org/apache/pinot/client/ResultSetGroupTest.java'
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix Assert.assertEquals calls that are too long
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
    print("Fixed final checkstyle line length violations!")
