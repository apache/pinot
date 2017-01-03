<div class="col-md-12">
  <div class="table-responsive">
    <table class="table table-bordered analysis-table">
      <thead>
      <tr>
        <th></th>
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
        <td>{{cKeyIndex}}</td>
        {{#each cumulativePercentageChangeArr as |cPercentageChange cidx|}}
        <td style="background-color: {{computeColor cPercentageChange}};color: {{computeTextColor cPercentageChange}};" id="{{cKeyIndex}}-{{cidx}}">
          <a id="{{cKeyIndex}}-href-{{cidx}}" >
          <div class="row">
            {{#if @root.showDetailsChecked}}
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.cumulativeCurrentValues cKeyIndex) cidx}}
            </div>
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.cumulativeBaselineValues cKeyIndex) cidx}}
            </div>
            {{/if}}
            <div class="col-md-4">
              {{cPercentageChange}}%
            </div>
          </div>
          </a>
        </td>
        {{/each}}
        </tr>
        {{/each}}
        {{/if_eq}}


        {{#if_eq this.showCumulativeChecked false}}
        {{#each this.subDimensionContributionDetails.percentageChange as |percentageChangeArr
        keyIndex|}}
      <tr>
        <td>{{keyIndex}}</td>
        {{#each percentageChangeArr as |percentageChange idx|}}
        <td style="background-color: {{computeColor percentageChange}};color: {{computeTextColor percentageChange}};" id="{{keyIndex}}-{{idx}}">
          <a id="{{keyIndex}}-href-{{idx}}">
          <div class="row" >
            {{#if @root.showDetailsChecked}}
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.currentValues keyIndex) idx}}
            </div>
            <div class="col-md-4">
              {{lookup (lookup @root.subDimensionContributionDetails.baselineValues keyIndex) idx}}
            </div>
            {{/if}}
            <div class="col-md-4">
              {{percentageChange}}%
            </div>
          </div>
        </td>
        </a>
        {{/each}}
      </tr>
      {{/each}}
      {{/if_eq}}
      </tbody>
    </table>
  </div>
</div>
