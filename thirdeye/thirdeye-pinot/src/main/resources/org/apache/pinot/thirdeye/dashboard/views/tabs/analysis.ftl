<div class="container-fluid ">
  <div class="row bg-white row-bordered">
    <div class="container">
      <div class="search-bar">

        <div class="search-title">
          <label for="metric-input" class="label-large-light">Metric Name: </label>
        </div>

        <div class="search-input search-field">
          <select style="width:100%" id="analysis-metric-input" class="label-large-light underlined"></select>
        </div>

        <a class="btn thirdeye-btn search-button" type="button" id="analysis-search-button">
          <span class="glyphicon glyphicon-search"></span>
          <span class="a11y-text">Search metric</span>
        </a>
      </div>
    </div>
  </div>
</div>

<div class="container top-buffer bottom-buffer">
  <div id="analysis-options-placeholder"></div>
  <div id='analysis-spin-area'></div>
  <div id="dimension-tree-map-placeholder"></div>
  <div class="spinner-wrapper">
    <div id="dimension-tree-spin-area"></div>
  </div>
  <div id="rootcause-table-placeholder"></div>
  <div id="rootcause-spin-area"></div>
</div>
