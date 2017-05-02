<div class="anomalies-panel">
  <div class="filter-header">
    Filter
  </div>

  <div class="filter-body">
    <div class="filter-section" id="#anomalies-time-range">
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
    </div>
    <div class="filter-section">
      <h5 class="label-medium-semibold filter-title">
        <span class="filter-title__name">Status</span>
        <span class="filter-title__action">+</span>
      </h5>
      <ul class="filter-body__list filter-body__list--hidden" id="statusFilterMap">
        {{#each statusFilterMap}}
           <li class="filter-item">
            <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" checked=true>
            <label for="{{@key}}" class="filter-item__label">{{@key}}</label>
            <span class="filter-item__count">{{this}}</span>
          </li>
        {{/each}}
      </ul>
    </div>
    <div class="filter-section">
      <h5 class="label-medium-semibold filter-title">
        <span class="filter-title__name">Functions</span>
        <span class="filter-title__action">+</span>
      </h5>
      <ul class="filter-body__list filter-body__list--hidden" id="functionFilterMap">
        {{#each functionFilterMap}}
           <li class="filter-item">
            <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" checked=true>
            <label for="{{@key}}" class="filter-item__label">{{@key}}</label>
            <span class="filter-item__count">{{this}}</span>
          </li>
        {{/each}}
      </ul>
    </div>
    <div class="filter-section">
      <h5 class="label-medium-semibold filter-title">
        <span class="filter-title__name">DataSets</span>
        <span class="filter-title__action">+</span>
      </h5>
      <ul class="filter-body__list filter-body__list--hidden" id="datasetFilterMap">
        {{#each datasetFilterMap}}
           <li class="filter-item">
            <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" checked=true>
            <label for="{{@key}}" class="filter-item__label">{{@key}}</label>
            <span class="filter-item__count">{{this}}</span>
          </li>
        {{/each}}
      </ul>
    </div>
    <div class="filter-section">
      <h5 class="label-medium-semibold filter-title">
        <span class="filter-title__name">Metric</span>
        <span class="filter-title__action">+</span>
      </h5>
      <ul class="filter-body__list filter-body__list--hidden" id="metricFilterMap">
        {{#each metricFilterMap}}
           <li class="filter-item">
            <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" checked=true>
            <label for="{{@key}}" class="filter-item__label">{{@key}}</label>
            <span class="filter-item__count">{{this}}</span>
          </li>
        {{/each}}
      </ul>
    </div>
    {{#each dimensionFilterMap}}
      <div class="filter-section">
      <h5 class="label-medium-semibold filter-title">
        <span class="filter-title__name">{{@key}}</span>
        <span class="filter-title__action">+</span>
      </h5>
        <ul class="filter-body__list filter-body__list--hidden" id="@key">
          {{#each this}}
             <li class="filter-item">
              <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" checked=true>
              <label for="{{@key}}" class="filter-item__label">{{@key}}</label>
              <span class="filter-item__count">{{this}}</span>
            </li>
          {{/each}}
        </ul>
      </div>
    {{/each}}
  </div>


  <div class="filter-footer">
    <a type="button" id="apply-button" class="thirdeye-link">Apply Filter</a>
  </div>
</div>
