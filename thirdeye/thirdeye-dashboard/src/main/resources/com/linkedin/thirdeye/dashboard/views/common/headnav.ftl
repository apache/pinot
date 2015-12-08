<nav class="uk-navbar">
    <ul class="view-links uk-navbar-nav">
        <li><a type="DIMENSION" view="TABULAR">Overview</a></li>
        <li><a type="DIMENSION" view="HEAT_MAP">Heatmap</a></li>
        <li><a type="DIMENSION" view="MULTI_TIME_SERIES">Contributors</a></li>
    </ul>
    <form class="uk-form uk-float-right uk-margin-large-right uk-margin-small-top">
        <div class="uk-button uk-form-select" data-uk-form-select>
            <span></span>
            <i class="uk-icon-caret-down"></i>
            <select id="landing-collection">
            <#--<#list collections as collection>
                <option value="${collection}">${collection}</option>
            </#list>-->
            </select>
        </div>
        <button id="landing-submit" class="uk-button uk-button-primary">Go</button>
    </form>
</nav>
