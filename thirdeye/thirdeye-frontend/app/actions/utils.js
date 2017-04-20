let TypeCache = {};

/**
 * Store into the TypeCache and return the type name.
 * @throws Will throw an error if the name is already defined.
 * @return {String} label - Action type name
 */
export function type(label) {
  if (TypeCache[label]) {
    throw new Error(`Action type "${label}" is not unique.`);
  }
  TypeCache[label] = true;
  return label;
}
