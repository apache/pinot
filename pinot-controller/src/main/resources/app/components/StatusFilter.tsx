import React from 'react';
import { FormControl, InputLabel, Select, MenuItem, Chip, makeStyles, createStyles } from '@material-ui/core';
import PropTypes from 'prop-types';

const useStyles = makeStyles(() =>
  createStyles({
    formControl: {
      marginLeft: 16,
      minWidth: 170,
    },
    chip: {
      height: 24,
      fontWeight: 600,
    },
    good: {
      color: '#4CAF50',
      border: '1px solid rgba(76,175,80,0.3)',
      backgroundColor: 'rgba(76,175,80,0.1)',
    },
    bad: {
      color: '#f44336',
      border: '1px solid rgba(244,67,54,0.3)',
      backgroundColor: 'rgba(244,67,54,0.1)',
    },
    consuming: {
      color: '#ff9800',
      border: '1px solid rgba(255,152,0,0.3)',
      backgroundColor: 'rgba(255,152,0,0.1)',
    },
    error: {
      color: '#a11',
      border: '1px solid rgba(170,17,17,0.3)',
      backgroundColor: 'rgba(170,17,17,0.1)',
    },
    menuItem: {
      '&:hover': {
        backgroundColor: 'rgba(66,133,244,0.08)',
      },
      '&.Mui-selected': {
        backgroundColor: 'rgba(66,133,244,0.12)',
      },
    },
  })
);

export const getStatusChipClass = (status: string, classes: Record<string, string>): string => {
  const s = status.toLowerCase();
  if (['good', 'online', 'alive', 'true'].includes(s)) return classes.good;
  if (['bad', 'offline', 'dead', 'false'].includes(s)) return classes.bad;
  if (s === 'error') return classes.error;
  return classes.consuming;
};

interface Option { label: string; value: string; }
interface StatusFilterProps {
  value: string;
  onChange: (value: string) => void;
  options: Option[];
  label?: string;
}

const StatusFilter = ({ value, onChange, options, label = 'Status' }: StatusFilterProps) => {
  const classes = useStyles();

  const handleChange = (e: React.ChangeEvent<{ value: unknown }>) => {
    onChange(e.target.value as string);
  };

  const renderValue = (selected: unknown) => {
    const option = options.find((o) => o.value === selected) || { label: selected as string, value: selected as string };
    const chipClass = getStatusChipClass(option.value, classes);
    return <Chip label={option.label} className={`${classes.chip} ${chipClass}`} variant="outlined" />;
  };

  return (
    <FormControl variant="outlined" size="small" className={classes.formControl}>
      <InputLabel id="status-filter-label">{label}</InputLabel>
      <Select
        labelId="status-filter-label"
        value={value}
        onChange={handleChange}
        label={label}
        renderValue={renderValue}
        MenuProps={{
          anchorOrigin: { vertical: 'bottom', horizontal: 'left' },
          transformOrigin: { vertical: 'top', horizontal: 'left' },
          getContentAnchorEl: null,
          PaperProps: { style: { maxHeight: 300, marginTop: 4 } },
        }}
      >
        {options.map((opt) => (
          <MenuItem key={opt.value} value={opt.value} className={classes.menuItem}>
            {opt.label}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

StatusFilter.propTypes = {
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  options: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string.isRequired,
      value: PropTypes.string.isRequired,
    })
  ).isRequired,
  label: PropTypes.string,
};

export default StatusFilter;
