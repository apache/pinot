<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        <th style="width: 10%;"></th>
        {{#each this.subDimensionContributionDetails.timeBucketsCurrent as |timestamp timeIndex|}}
        <th>{{displayMonthDayHour timestamp}}</th>
        {{/each}}
      </tr>
      </thead>

      <tbody>
      {{#if this.showDetailsChecked}}
      <tr>
        <td></td>
        {{#each this.subDimensionContributionDetails.timeBucketsCurrent}}
        <td>
          <div class="row">
            <div class="col-md-4">
              Current
            </div>
            <div class="col-md-4">
              Baseline
            </div>
            <div class="col-md-4">
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
        <td><div style="overflow:auto">{{cKeyIndex}}</div></td>
        {{#each cumulativePercentageChangeArr as |cPercentageChange cidx|}}
        <td style="background-color: {{computeColor cPercentageChange}};color: {{computeTextColor cPercentageChange}};">
          <div class="row">
            {{#if @root.showDetailsChecked}}
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.cumulativeCurrentValues
              cKeyIndex) cidx}}
            </div>
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.cumulativeBaselineValues
              cKeyIndex) cidx}}
            </div>
            <div class="col-md-4">
            {{else}}
            <div class="col-md-12">
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
        <td><div style="overflow:auto">{{keyIndex}}</div></td>
        {{#each percentageChangeArr as |percentageChange idx|}}
        <td style="background-color: {{computeColor percentageChange}};color: {{computeTextColor percentageChange}};">
          <div class="row">
            {{#if @root.showDetailsChecked}}
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.currentValues keyIndex) idx}}
            </div>
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.baselineValues keyIndex) idx}}
            </div>
            <div class="col-md-4">
            {{else}}
            <div class="col-md-12">
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
