<script src="/assets/js/thirdeye.dimension.timeseries.js"></script>

<div class="title-box uk-clearfix">
    <label>Dimension:</label>
    <div  class="uk-button uk-form-select uk-left" data-uk-form-select>
        <span>Dimension</span>
        <i class="uk-icon-caret-down"></i>
        <select class="section-selector">
        <#list dimensionView.view.dimensions as dimension>
            <option value="${dimension}">${dimension}</option>
        </#list>
        </select>
    </div>
    <form class="time-input-form uk-form uk-form-stacked uk-float-right">
        <div class="time-input-form-error uk-alert uk-alert-danger hidden">
            <p></p>
        </div>
        <div class="uk-margin-small-top uk-margin-bottom">
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Current Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input class="time-input-form-current-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Baseline Date
                </label>
                <div class="uk-form-icon">
                    <i class="uk-icon-calendar"></i>
                    <input class="time-input-form-baseline-date" type="text" data-uk-datepicker="{format:'YYYY-MM-DD'}">
                </div>
            </div>
            <div class="uk-display-inline-block">
                <label class="uk-form-label">
                    Baseline Granularity
                </label>
                <div  class="uk-button-group" data-uk-button-radio>
                    <button type="button" class="baseline-unit uk-button "  value="3600000" >hour(s)</button>
                    <button type="button" class="baseline-unit uk-button"  value="86400000" >day(s)</button>
                </div>
            </div>
            <div class="uk-display-inline-block uk-margin-right">
                <button type="submit" class="time-input-form-submit uk-button uk-button-small uk-button-primary ">Go</button>
            </div>
        </div>
    </form>
</div>
<div id="dimension-time-series-area">
    <#list dimensionView.view.dimensions as dimension>
        <div class="section-wrapper" rel="${dimension}">
            <h3 class="dimension-time-series-title" dimension="${dimension}"> ${dimension}</h3>
            <div class="dimension-time-series-placeholder" dimension="${dimension}"></div>
            <div class="dimension-time-series-tooltip" dimension="${dimension}"></div>
            <div class="dimension-time-series-legend time-series-legend" dimension="${dimension}"></div>
        </div>
    </#list>
</div>
