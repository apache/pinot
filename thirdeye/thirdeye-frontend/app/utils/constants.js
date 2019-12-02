// Share constant file
export const deleteProps = {
  method: 'delete',
  headers: { 'content-type': 'Application/Json' },
  credentials: 'include'
};

export const toastOptions = {
  timeOut: 10000
};

export default {
  deleteProps,
  toastOptions
};
