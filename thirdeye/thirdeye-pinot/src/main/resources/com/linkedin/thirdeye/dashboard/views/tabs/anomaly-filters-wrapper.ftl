<aside class="anomalies-panel">
  <div class="filter-header">
    Filter
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
      <!-- To do: make spinner wrapper expand -->
      <div class="spinner-wrapper">
        <div id="anomaly-filter-spinner"></div>
      </div>
      <div id="anomaly-filters-place-holder"></div>
    </section>
  </div>
  <div class="filter-footer">
    <a type="button" id="clear-button" class="thirdeye-link">Clear</a>
    <a type="button" id="apply-button" class="thirdeye-link">Apply Filter</a>
  </div>
</aside>
