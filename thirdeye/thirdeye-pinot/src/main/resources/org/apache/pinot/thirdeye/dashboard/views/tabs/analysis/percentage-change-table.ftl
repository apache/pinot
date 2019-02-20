<div class="col-xs-12">
  <div class="table-responsive analysis-table-wrapper ">
    <table class="table table-bordered analysis-table tree-map__table">
      <thead>
      <tr>
        <th class="analysis-table__anchor"></th>
        {{#each this.subDimensionContributionDetails.timeBucketsCurrent as |timestamp timeIndex|}}
        <th class="analysis-table__header">
          {{formatDate ../granularity timestamp}}
        </th>
        {{/each}}
      </tr>
      </thead>

      <tbody>
      {{#if this.showDetailsChecked}}
      <tr>
        <td class="analysis-table__anchor analysis-table__anchor--low"></td>
        {{#each this.subDimensionContributionDetails.timeBucketsCurrent}}
        <td class="analysis-table__subheader">
          <div class="analysis-table__cell">
            <div class="analysis-table__item">
              Current
            </div>
            <div class="analysis-table__item">
              Baseline
            </div>
            <div class="analysis-table__item analysis-table__item--small">
              Ratio
            </div>
          </div>
        </td>
        {{/each}}
      </tr>
      {{/if}}

      {{#if_eq this.showCumulativeChecked true}}
      {{#each this.subDimensionContributionDetails.cumulativePercentageChange as
      |cumulativePercentageChangeArr cKeyIndex|}}
      <tr>
        <td class="analysis-table__dimension">
          <div class="analysis-table__cell">{{cKeyIndex}}</div>
        </td>
        {{#each cumulativePercentageChangeArr as |cPercentageChange cidx|}}
        <td style="background-color: {{computeColor cPercentageChange @root.inverseMetric}};color: {{computeTextColor cPercentageChange}};">
          <div class="analysis-table__cell">
            {{#if @root.showDetailsChecked}}
            <div class="analysis-table__item">
              {{formatNumber (lookup (lookup @root.subDimensionContributionDetails.cumulativeCurrentValues
              cKeyIndex) cidx)}}
            </div>
            <div class="analysis-table__item">
              {{formatNumber (lookup (lookup @root.subDimensionContributionDetails.cumulativeBaselineValues
              cKeyIndex) cidx)}}
            </div>
            <div class="analysis-table__item analysis-table__item--small">
            {{else}}
            <div class="analysis-table__item  analysis-table__item--large">
            {{/if}}
              <a id="{{lookup @root.subDimensionsIndex cKeyIndex}}-{{cidx}}"
                 style="color:{{computeTextColor cPercentageChange}}">
                {{cPercentageChange}}%
              </a>
            </div>
          </div>
        </td>
        {{/each}}
      </tr>
      {{/each}}
      {{/if_eq}}


      {{#if_eq this.showCumulativeChecked false}}
      {{#each this.subDimensionContributionDetails.percentageChange as |percentageChangeArr
      keyIndex|}}
      <tr>
        <td class="analysis-table__dimension">
          <div class="analysis-table__cell">{{keyIndex}}</div>
        </td>
        {{#each percentageChangeArr as |percentageChange idx|}}
        <td style="background-color: {{computeColor percentageChange @root.inverseMetric}};color: {{computeTextColor percentageChange}};">
          <div class="analysis-table__cell">
            {{#if @root.showDetailsChecked}}
            <div class="analysis-table__item">
              {{formatNumber (lookup (lookup @root.subDimensionContributionDetails.currentValues keyIndex) idx)}}
            </div>
            <div class="analysis-table__item">
              {{formatNumber (lookup (lookup @root.subDimensionContributionDetails.baselineValues keyIndex) idx)}}
            </div>
            <div class="analysis-table__item analysis-table__item--small">
            {{else}}
            <div class="analysis-table__item  analysis-table__item--large">
            {{/if}}
              <a id="{{lookup @root.subDimensionsIndex keyIndex}}-{{idx}}"
                 style="color:{{computeTextColor percentageChange}}">
                {{percentageChange}}%
              </a>
            </div>
          </div>
        </td>
        {{/each}}
      </tr>
      {{/each}}
      {{/if_eq}}
      </tbody>
    </table>
  </div>
</div>
