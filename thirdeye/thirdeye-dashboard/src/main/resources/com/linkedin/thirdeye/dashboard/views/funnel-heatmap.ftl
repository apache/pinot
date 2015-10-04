<script src="/assets/js/thirdeye.funnelheatmap.js"></script>

<#if (funnelViews?size > 0)>
<div id="custom-funnels-container">
    <div class="title-box uk-clearfix">
        <div class="uk-display-inline-block">
            <div class="funnel-cummulative" data-uk-button-checkbox><button type="button" id="funnel-cummulative" class="uk-button">Cummulative</button>
            </div>
        </div>
        <ul class="dimension-combination uk-display-inline-block" >Filters:
        </ul>
        <form class="time-input-form uk-form uk-form-stacked uk-float-right">
            <div id="funnel-form-error" class="uk-alert uk-alert-danger hidden">
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
                        Baseline Granularity
                    </label>
                    <div  class="uk-button-group" data-uk-button-radio>
                        <button type="button" class="baseline-unit uk-button "  value="3600000" >hour(s)</button>
                        <button type="button" class="baseline-unit uk-button"  value="86400000" >day(s)</button>
                    </div>
                </div>
                <div class="uk-display-inline-block">
                    <div class="uk-button-group" data-uk-button-radio>
                        <button type="button" class="funnel-moving-average-size uk-button" unit="WoW" value="7">WoW</button>
                        <button type="button" class="funnel-moving-average-size uk-button" unit="Wo2W" value="14" >Wo2W</button>
                        <button type="button" class="funnel-moving-average-size uk-button" unit="Wo4W" value="28">Wo4W</button>
                    </div>
                </div>
                <div class="uk-display-inline-block uk-margin-right">
                    <button type="submit" class="time-input-form-submit uk-button uk-button-small uk-button-primary ">Go</button>
                </div>
            </div>
        </form>
        <ul class="uk-tab funnel-tabs" data-uk-tab>
            <li class="uk-active"><a href="#">Ratio</a></li>
            <li><a href="#">Detailed Data</a></li>
        </ul>
    </div>
</div>
<div id="custom-funnel-section">
    <i class="uk-icon-spin uk-icon-large"></i>
</div>
<div id="funnel-thumbnails" class="funnel-thumbnail-container uk-margin-bottom" data-uk-grid data-uk-slider>
    <#list funnelViews as funnel>
        <div class="funnel">
            <h3 class="metric-list">${funnel.name} </h3>
            <h3 class="metric-list"> (current = ${funnel.current} & baseline = ${funnel.baseline})
            </h3>

                <table class="uk-table dimension-view-heat-map-rendered">
                    <thead>
                        <tr>
                            <th class="metric-label">Hour</th>
                            <#list funnel.aliasToActualMap?keys as key>
                                <th class="metric-label">${key}</th>
                            </#list>
                        </tr>
                    </thead>
                    <tbody>
                    <#list funnel.table as row>
                        <tr>
                            <td>${row.first}</td>
                            <#list row.second as column>
                                <#if (column??)>
                                    <td
                                            class="heat-map-cell custom-tooltip"
                                            tooltip="${column}"
                                            value="${column}"
                                            >${(column * 100)?string["0.0"]}%</td>
                                <#else>
                                    <td class="not-available">N/A</td>
                                </#if>
                            </#list>
                        </tr>
                    </#list>
                    </tbody>

                </table>

        </div>
    </#list>


</div>
</#if>
