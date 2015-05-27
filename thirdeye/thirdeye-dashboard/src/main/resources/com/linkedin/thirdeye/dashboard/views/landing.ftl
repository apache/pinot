<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <#include "common/style.ftl">
        <#include "common/script.ftl">
        <script src="/assets/js/thirdeye.landing.js"></script>
    </head>
    <body>
        <div id="landing-area">
            <img src="/assets/img/chakra.gif" alt="ThirdEye"/>

            <!-- Collections drop-down -->
            <form class="uk-form">
                <div class="uk-button uk-form-select" data-uk-form-select>
                    <span></span>
                    <i class="uk-icon-caret-down"></i>
                    <select id="landing-collection">
                        <#list collections as collection>
                            <option value="${collection}">${collection}</option>
                        </#list>
                    </select>
                </div>
                <button id="landing-submit" class="uk-button uk-button-primary">Go</button>
            </form>
        </div>
    </body>
</html>
