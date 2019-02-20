<section id="tabs-section">
<script id="tab-template" type="text/x-handlebars-template">
<section id="{{tabName}}-section"  class="uk-grid">

    <div id="{{tabName}}-form-area" class="form-area uk-clearfix uk-width-1-4">
    </div>
    <div id="{{tabName}}-chart-area" class="chart-area uk-clearfix uk-width-3-4">
        <div id="{{tabName}}-display-chart-area" class="display-chart-area" style="position: relative;">
            <div id="{{tabName}}-chart-area-error" class="uk-alert uk-alert-danger hidden"></div>
            <div id="{{tabName}}-chart-area-loader" class="loader hidden">
                <i class="uk-icon-spinner uk-icon-spin uk-icon-large"></i>
            </div>
            {{#if showChartSection}}
            <section id="{{tabName}}-display-chart-section" class="display-chart-section">
            </section>
            {{/if}}
            {{#if showSelfServiceForms}}
            <section id="{{tabName}}-display-main-content-section" class="display-main-content-section">
                <#include "self-service-tab/self-service.ftl">
            </section>
            {{/if}}


        </div>
    </div>

</section>
</script>
</section>