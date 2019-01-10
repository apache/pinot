<aside class="anomalies-panel">
  <div class="filter-header">
    Filter
    <a type="button" id="clear-button" class="thirdeye-link">
      <span class="glyphicon glyphicon-trash" area-hidden=true title="clear filters"></span>
    </a>
  </div>

  <div class="filter-body">
    <section class="filter-section filter-section--no-border" id="anomalies-time-range">
      <div class="datepicker-field">
        <h5 class="label-medium-semibold">Start date</h5>
        <div id="anomalies-time-range-start" class="datepicker-range">
          <span></span>
          <b class="caret"></b>
        </div>
      </div>
      <div class="datepicker-field">
        <h5 class="label-medium-semibold">End date</h5>
        <div id="anomalies-time-range-end" class="datepicker-range">
          <span></span>
          <b class="caret"></b>
        </div>
      </div>
    </section>
    <section>
      <div class="spinner-wrapper">
        <div id="anomaly-filter-spinner"></div>
      </div>
      <div id="anomaly-filters-place-holder"></div>
    </section>
  </div>
  <div class="filter-footer">
    <a type="button" id="apply-button" class="thirdeye-link">Apply</a>
  </div>
</aside>
