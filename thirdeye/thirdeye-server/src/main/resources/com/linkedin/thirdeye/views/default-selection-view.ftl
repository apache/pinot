<!DOCTYPE html>
<html>
    <head>
        <link rel="stylesheet" href="/assets/stylesheets/uikit/uikit.almost-flat.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/uikit/components/form-select.min.css"/>
        <link rel="stylesheet" href="/assets/stylesheets/dashboard.css"/>

        <script src="/assets/javascripts/vendor/jquery.js"></script>
        <script src="/assets/javascripts/uikit/uikit.min.js"></script>
        <script src="/assets/javascripts/uikit/components/form-select.min.js"></script>
    </head>
    <body>
        <div id="selection-area">
            <img src="/assets/images/chakra.gif"/>

            <form class="uk-form">
                <div class="uk-button uk-form-select" data-uk-form-select>
                    <span></span>
                    <i class="uk-icon-caret-down"></i>
                    <select id="collection-select">
                        <#list collections as collection>
                            <option value="${collection}">${collection}</option>
                        </#list>
                    </select>
                </div>
                <button id="select-go" class="uk-button uk-button-primary" type="button">Go</button>
            </form>
        </div>

        <script>
        $(document).ready(function() {
            $("#select-go").click(function() {
                window.location = "/dashboard/" + $("#collection-select").val()
            })
        })
        </script>
    </body>
</html>
