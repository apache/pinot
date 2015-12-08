<nav class="uk-navbar">
    <ul class="view-links uk-navbar-nav">
        <li><a type="DIMENSION" view="TABULAR">Overview</a></li>
        <li><a type="DIMENSION" view="HEAT_MAP">Heatmap</a></li>
        <li><a type="DIMENSION" view="MULTI_TIME_SERIES">Contributors</a></li>
    </ul>
    <form class="uk-form uk-float-right uk-margin-large-right uk-margin-small-top">
        <label><b>Dataset:</b></label>
        <div class="uk-button uk-form-select" data-uk-form-select>
            <span id="collection-name-display"></span>
            <i class="uk-icon-caret-down"></i>
            <select id="landing-collection">
            <script id="collections-template" type="text/x-handlebars-template">
                {{#each this}}
                <option class="collection-option" value="{{this}}">{{this}}</option>
                {{/each}}
            </script>
            </select>
        </div>
        <button type="submit" id="landing-submit" class="uk-button uk-button-primary" disabled>Go</button>
    </form>
</nav>
