<html>
    <head></head>
    <body>
        <p>${cause}</p>
        <ul>
            <#list stackTrace as stackTraceElement>
                <li>${stackTraceElement}</li>
            </#list>
        </ul>
    </body>
</html>
