<script id="metric-list-template" type="text/x-handlebars-template">
    {{#with data}}
    {{#each this}}
    <li class=
    {{#if @root/singleMetricSelector}}
        'single-metric-option{{@root/scope}}'
    {{else}}
        'metric-option'
    {{/if}}
    rel='metrics' value='{{this}}'><a href='#' class='uk-dropdown-close'>{{this}}</a></li>
    {{/each}}
    {{/with}}
</script>