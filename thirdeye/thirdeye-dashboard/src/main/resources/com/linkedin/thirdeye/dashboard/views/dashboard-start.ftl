<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/style.ftl">
        <#include "common/script.ftl">
        <script src="/assets/js/thirdeye.sidenav.js"></script>
    </head>
    <body>
        <div class="uk-grid">
            <#-- Side nav -->
            <div id="dashboard-sidenav" class="uk-width-1-4">
                <#include "common/sidenav.ftl">
            </div>

            <div id="dashboard-start" class="uk-width-3-4">
                <div id="dashboard-start-logo">
                    <img src="/assets/img/chakra.gif"></img>
                </div>
            </div>
        </div>
    </body>
</html>
