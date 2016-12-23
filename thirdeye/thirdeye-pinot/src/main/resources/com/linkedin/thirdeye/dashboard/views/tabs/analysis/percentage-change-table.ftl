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
      {{#if_eq this.showCumulativeChecked true}}
      {{#each this.subDimensionContributionDetails.cumulativePercentageChange as |cumulativePercentageChangeArr cKeyIndex|}}
      <tr>
        <td>{{cKeyIndex}}</td>
        {{#each cumulativePercentageChangeArr as |cPercentageChange cidx|}}
        <td style="background-color: {{computeColor cPercentageChange}};color: {{computeTextColor cPercentageChange}};" id="{{cKeyIndex}}-{{cidx}}">{{cPercentageChange}}%
        </td>
        {{/each}}
      </tr>
      {{/each}}
      {{/if_eq}}


     {{#if_eq this.showCumulativeChecked false}}
      {{#each this.subDimensionContributionDetails.percentageChange as |percentageChangeArr keyIndex|}}
      <tr>
        <td>{{keyIndex}}</td>
        {{#each percentageChangeArr as |percentageChange idx|}}
        <td style="background-color: {{computeColor percentageChange}};color: {{computeTextColor percentageChange}};" id="{{keyIndex}}-{{idx}}">{{percentageChange}}%
        </td>
        {{/each}}
      </tr>
      {{/each}}
      {{/if_eq}}

      </tbody>
    </table>
  </div>
</div>
