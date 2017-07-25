<div class="analysis-card top-buffer">
  <div class="analysis-card__header">
    <a class="thirdeye-link thirdeye-link--nav thirdeye-link--active">Dimension Analysis</a>
    {{!-- <a class="thirdeye-link thirdeye-link--nav" target="_blank" href={{rcaMetricUrl}}>Metric Correlation</a> --}}
  </div>
  <div class="analysis-card__body padding-all">
    <h4 class="analysis-title bottom-buffer">Dimension Analysis
      <span class="analysis-details"> (for <label class="label-medium-semibold">Metric</label> {{metricName}} and <label class="label-medium-semibold">Dimensions</label>

      {{#each heatMapFilters}}
        {{@key}}:
        {{#each this as |dimension|}}
          {{dimension}}{{#unless @last}}, {{/unless}}
        {{/each}}{{#unless @last}}/ {{/unless}}
      {{else}}
        All
      {{/each}})
      </span>
    </h4>
    <div class="contribution-analysis">
      <div class="contribution-analysis__daterangepicker">
        <h5 class="label-medium-semibold">Comparing:</h5>
        <div class="datepicker-range" id="heatmap-current-range">
          <span></span>
          <b class="caret"></b>
        </div>
      </div>
      <div class="contribution-analysis__daterangepicker">
        <h5 class="label-medium-semibold">To:</h5>
        <div class="datepicker-range" id="heatmap-baseline-range">
          <span></span>
          <b class="caret"></b>
        </div>
      </div>
    </div>

    <div class="analysis-card top-buffer">
      <div class="analysis-card__header analysis-card__header--small">
        <div class="row">
          <div class="col-xs-12">
            <nav class="navbar navbar-transparent navbar--no-border" role="navigation">
              <div class="collapse navbar-collapse tree-map__nav">
                <ul class="nav navbar-nav tree-map-tabs" id="dashboard-tabs">
                  <li class="tree-map__tab active" id="percent_change">
                    <a class="tree-map__link" href="#percent_change" data-toggle="tab">% Change</a>
                  </li>
                  <li class="tree-map__tab" id="change_in_contribution">
                    <a class="tree-map__link" href="#change_in_contribution" data-toggle="tab">Change in contribution</a>
                  </li>
                  <li class="tree-map__tab" id="contribution_to_overall_change">
                    <a class="tree-map__link" href="#contribution_to_overall_change" data-toggle="tab">Contribution to overall change</a>
                  </li>
                </ul>
              </div>
            </nav>
          </div>
        </div>
      </div>
      <div class="analysis-card__body padding-all">
        <div id="dimension-tree-map-graph-placeholder"></div>
        <div>
      </div>
  </div>
</div>
