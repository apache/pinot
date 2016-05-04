<script id="datasets-template" type="text/x-handlebars-template">
    <ul class="uk-nav uk-nav-dropdown single-select dataset-options radio-options">
        {{#each this}}
        <li class="dataset-option" rel="dataset" value="{{this}}"><a href="#">{{this}}</a></li>
        {{/each}}
    </ul>
</script>