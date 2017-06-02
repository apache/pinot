import Ember from 'ember';

export function computeColor(params/*, hash*/) {
  const value = params[0];
  const opacity = Math.abs(value / 25);

  if (value >0){
    return `rgba(0,0,234,${opacity})`;
  } else{
    return `rgba(234,0,0,${opacity})`;
  }
}

export default Ember.Helper.helper(computeColor);
