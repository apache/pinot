<#-- @ftlvariable name="" type="com.linkedin.thirdeye.views.CollectionsView" -->
<!DOCTYPE html>

<html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">

        <link rel="stylesheet" href="/assets/stylesheets/normalize.css" />
        <link rel="stylesheet" href="/assets/stylesheets/foundation.css" />
        <link rel="stylesheet" href="/assets/stylesheets/collections-view.css" />

        <script src="/assets/javascripts/vendor/modernizr.js"></script>

        <title>ThirdEye</title>
    </head>

    <body>
        <img src="/assets/images/chakra.gif"/>

        <form>
            <div class="row">
                <div class="large-12 columns">
                    <div class="row collapse">
                        <div class="small-10 columns">
                            <select id="select-collection">
                                <#list collections as collection>
                                    <option value="${collection}">${collection}</option>
                                </#list>
                            </select>
                        </div>
                        <div class="small-2 columns">
                            <a href="#" class="button postfix" id="select-collection-button">Go</a>
                        </div>
                    </div>
                </div>
            </div>
        </form>
    </body>

    <script src='/assets/javascripts/vendor/jquery.js'></script>

    <script>
        $(document).ready(function() {
            $("#select-collection-button").click(function() {
                document.location.href = '/dashboard/' + encodeURIComponent($("#select-collection").val())
            })
        })
    </script>
</html>
