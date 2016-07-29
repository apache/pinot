<script id="metric-list-template" type="text/x-handlebars-template">
    {{#with data}}
    {{#each this}}
    <li class='metric-option'
    rel='metrics' value='{{this}}'><a href='#' class='uk-dropdown-close'>{{this}}</a></li>
    {{/each}}
    {{/with}}
</script>