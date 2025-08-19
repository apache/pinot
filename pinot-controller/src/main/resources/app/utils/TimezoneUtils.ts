/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import moment from 'moment';
import 'moment-timezone';

// Default timezone fallback (single source of truth)
export const DEFAULT_TIMEZONE_FALLBACK = 'UTC';
// Default timezone
export const DEFAULT_TIMEZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || DEFAULT_TIMEZONE_FALLBACK;

// Standardized timezones using 3-letter zone IDs with location information
// Each timezone has only one representation, sorted by UTC offset
export const STANDARD_TIMEZONES = [
  // UTC-12 to UTC-1
  { value: 'BIT(Pacific/Baker)', label: 'BIT (UTC-12)' },
  { value: 'SST(Pacific/Samoa)', label: 'SST (UTC-11)' },
  { value: 'HST(Pacific/Honolulu)', label: 'HST (UTC-10)' },
  { value: 'HAST(America/Adak)', label: 'HAST (UTC-10)' },
  { value: 'AKST(America/Anchorage)', label: 'AKST (UTC-9)' },
  { value: 'HADT(America/Adak)', label: 'HADT (UTC-9)' },
  { value: 'PST(America/Los_Angeles)', label: 'PST (UTC-8)' },
  { value: 'AKDT(America/Anchorage)', label: 'AKDT (UTC-8)' },
  { value: 'MST(America/Denver)', label: 'MST (UTC-7)' },
  { value: 'PDT(America/Los_Angeles)', label: 'PDT (UTC-7)' },
  { value: 'CST(America/Chicago)', label: 'CST (UTC-6)' },
  { value: 'MDT(America/Denver)', label: 'MDT (UTC-6)' },
  { value: 'EST(America/New_York)', label: 'EST (UTC-5)' },
  { value: 'CDT(America/Chicago)', label: 'CDT (UTC-5)' },
  { value: 'COT(America/Bogota)', label: 'COT (UTC-5)' },
  { value: 'PET(America/Lima)', label: 'PET (UTC-5)' },
  { value: 'AST(America/Puerto_Rico)', label: 'AST (UTC-4)' },
  { value: 'EDT(America/New_York)', label: 'EDT (UTC-4)' },
  { value: 'VET(America/Caracas)', label: 'VET (UTC-4)' },
  { value: 'BOT(America/La_Paz)', label: 'BOT (UTC-4)' },
  { value: 'ART(America/Argentina/Buenos_Aires)', label: 'ART (UTC-3)' },
  { value: 'BRT(America/Sao_Paulo)', label: 'BRT (UTC-3)' },
  { value: 'UYT(America/Montevideo)', label: 'UYT (UTC-3)' },
  { value: 'GFT(America/Cayenne)', label: 'GFT (UTC-3)' },
  { value: 'FNT(America/Noronha)', label: 'FNT (UTC-2)' },
  { value: 'AZOT(Atlantic/Azores)', label: 'AZOT (UTC-1)' },
  { value: 'CVT(Atlantic/Cape_Verde)', label: 'CVT (UTC-1)' },

  // UTC+0
  { value: 'UTC', label: 'UTC (UTC+0)' },
  { value: 'GMT', label: 'GMT (UTC+0)' },
  { value: 'WET(Europe/London)', label: 'WET (UTC+0)' },

  // UTC+1 to UTC+3
  { value: 'CET(Europe/Paris)', label: 'CET (UTC+1)' },
  { value: 'WEST(Europe/London)', label: 'WEST (UTC+1)' },
  { value: 'BST(Europe/London)', label: 'BST (UTC+1)' },
  { value: 'WAT(Africa/Lagos)', label: 'WAT (UTC+1)' },
  { value: 'CAT(Africa/Harare)', label: 'CAT (UTC+2)' },
  { value: 'CEST(Europe/Paris)', label: 'CEST (UTC+2)' },
  { value: 'EET(Europe/Athens)', label: 'EET (UTC+2)' },
  { value: 'SAST(Africa/Johannesburg)', label: 'SAST (UTC+2)' },
  { value: 'IST(Asia/Jerusalem)', label: 'IST (UTC+2)' },
  { value: 'EAT(Africa/Nairobi)', label: 'EAT (UTC+3)' },
  { value: 'EEST(Europe/Athens)', label: 'EEST (UTC+3)' },
  { value: 'MSK(Europe/Moscow)', label: 'MSK (UTC+3)' },
  { value: 'AST(Asia/Riyadh)', label: 'AST (UTC+3)' },

  // UTC+4 to UTC+6
  { value: 'GST(Asia/Dubai)', label: 'GST (UTC+4)' },
  { value: 'AMT(Asia/Yerevan)', label: 'AMT (UTC+4)' },
  { value: 'AZT(Asia/Baku)', label: 'AZT (UTC+4)' },
  { value: 'PKT(Asia/Karachi)', label: 'PKT (UTC+5)' },
  { value: 'UZT(Asia/Tashkent)', label: 'UZT (UTC+5)' },
  { value: 'YEKT(Asia/Yekaterinburg)', label: 'YEKT (UTC+5)' },
  { value: 'IST(Asia/Kolkata)', label: 'IST (UTC+5:30)' },
  { value: 'NPT(Asia/Kathmandu)', label: 'NPT (UTC+5:45)' },
  { value: 'BST(Asia/Dhaka)', label: 'BST (UTC+6)' },
  { value: 'BDT(Asia/Dhaka)', label: 'BDT (UTC+6)' },
  { value: 'BTT(Asia/Thimphu)', label: 'BTT (UTC+6)' },

  // UTC+7 to UTC+9
  { value: 'ICT(Asia/Bangkok)', label: 'ICT (UTC+7)' },
  { value: 'WIB(Asia/Jakarta)', label: 'WIB (UTC+7)' },
  { value: 'KRAT(Asia/Krasnoyarsk)', label: 'KRAT (UTC+7)' },
  { value: 'CST(Asia/Shanghai)', label: 'CST (UTC+8)' },
  { value: 'SGT(Asia/Singapore)', label: 'SGT (UTC+8)' },
  { value: 'HKT(Asia/Hong_Kong)', label: 'HKT (UTC+8)' },
  { value: 'MYT(Asia/Kuala_Lumpur)', label: 'MYT (UTC+8)' },
  { value: 'WITA(Asia/Makassar)', label: 'WITA (UTC+8)' },
  { value: 'IRKT(Asia/Irkutsk)', label: 'IRKT (UTC+8)' },
  { value: 'JST(Asia/Tokyo)', label: 'JST (UTC+9)' },
  { value: 'KST(Asia/Seoul)', label: 'KST (UTC+9)' },
  { value: 'WIT(Asia/Jayapura)', label: 'WIT (UTC+9)' },
  { value: 'YAKT(Asia/Yakutsk)', label: 'YAKT (UTC+9)' },

  // UTC+9:30 to UTC+12
  { value: 'ACST(Australia/Adelaide)', label: 'ACST (UTC+9:30)' },
  { value: 'AEST(Australia/Sydney)', label: 'AEST (UTC+10)' },
  { value: 'AEDT(Australia/Sydney)', label: 'AEDT (UTC+11)' },
  { value: 'SBT(Pacific/Guadalcanal)', label: 'SBT (UTC+11)' },
  { value: 'NCT(Pacific/Noumea)', label: 'NCT (UTC+11)' },
  { value: 'VUT(Pacific/Efate)', label: 'VUT (UTC+11)' },
  { value: 'NZST(Pacific/Auckland)', label: 'NZST (UTC+12)' },
  { value: 'FJT(Pacific/Fiji)', label: 'FJT (UTC+12)' },
  { value: 'TVT(Pacific/Funafuti)', label: 'TVT (UTC+12)' },
  { value: 'NZDT(Pacific/Auckland)', label: 'NZDT (UTC+13)' },
  { value: 'TOT(Pacific/Tongatapu)', label: 'TOT (UTC+13)' },
  { value: 'LINT(Pacific/Kiritimati)', label: 'LINT (UTC+14)' },
];

// Legacy support - map common location-based timezones to standardized format
const TIMEZONE_MAPPING: Record<string, string> = {
  // US Timezones
  'America/New_York': 'EST(America/New_York)',
  'America/Chicago': 'CST(America/Chicago)',
  'America/Denver': 'MST(America/Denver)',
  'America/Los_Angeles': 'PST(America/Los_Angeles)',
  'America/Anchorage': 'AKST(America/Anchorage)',
  'America/Adak': 'HAST(America/Adak)',
  'Pacific/Honolulu': 'HST(Pacific/Honolulu)',
  'America/Puerto_Rico': 'AST(America/Puerto_Rico)',

  // South American Timezones
  'America/Sao_Paulo': 'BRT(America/Sao_Paulo)',
  'America/Noronha': 'FNT(America/Noronha)',
  'America/Argentina/Buenos_Aires': 'ART(America/Argentina/Buenos_Aires)',
  'America/Montevideo': 'UYT(America/Montevideo)',
  'America/Cayenne': 'GFT(America/Cayenne)',
  'America/Bogota': 'COT(America/Bogota)',
  'America/Lima': 'PET(America/Lima)',
  'America/Caracas': 'VET(America/Caracas)',
  'America/La_Paz': 'BOT(America/La_Paz)',

  // Atlantic Timezones
  'Atlantic/Azores': 'AZOT(Atlantic/Azores)',
  'Atlantic/Cape_Verde': 'CVT(Atlantic/Cape_Verde)',

  // African Timezones
  'Africa/Lagos': 'WAT(Africa/Lagos)',
  'Africa/Harare': 'CAT(Africa/Harare)',
  'Africa/Johannesburg': 'SAST(Africa/Johannesburg)',
  'Africa/Nairobi': 'EAT(Africa/Nairobi)',

  // European Timezones
  'Europe/London': 'WET(Europe/London)',
  'Europe/Paris': 'CET(Europe/Paris)',
  'Europe/Berlin': 'CET(Europe/Paris)',
  'Europe/Athens': 'EET(Europe/Athens)',
  'Europe/Moscow': 'MSK(Europe/Moscow)',

  // Middle Eastern Timezones
  'Asia/Jerusalem': 'IST(Asia/Jerusalem)',
  'Asia/Riyadh': 'AST(Asia/Riyadh)',
  'Asia/Dubai': 'GST(Asia/Dubai)',

  // Central Asian Timezones
  'Asia/Yerevan': 'AMT(Asia/Yerevan)',
  'Asia/Baku': 'AZT(Asia/Baku)',
  'Asia/Karachi': 'PKT(Asia/Karachi)',
  'Asia/Tashkent': 'UZT(Asia/Tashkent)',
  'Asia/Yekaterinburg': 'YEKT(Asia/Yekaterinburg)',
  'Asia/Kolkata': 'IST(Asia/Kolkata)',
  'Asia/Kathmandu': 'NPT(Asia/Kathmandu)',
  'Asia/Dhaka': 'BDT(Asia/Dhaka)',
  'Asia/Thimphu': 'BTT(Asia/Thimphu)',

  // Southeast Asian Timezones
  'Asia/Bangkok': 'ICT(Asia/Bangkok)',
  'Asia/Jakarta': 'WIB(Asia/Jakarta)',
  'Asia/Makassar': 'WITA(Asia/Makassar)',
  'Asia/Jayapura': 'WIT(Asia/Jayapura)',
  'Asia/Krasnoyarsk': 'KRAT(Asia/Krasnoyarsk)',
  'Asia/Irkutsk': 'IRKT(Asia/Irkutsk)',
  'Asia/Yakutsk': 'YAKT(Asia/Yakutsk)',

  // East Asian Timezones
  'Asia/Shanghai': 'CST(Asia/Shanghai)',
  'Asia/Singapore': 'SGT(Asia/Singapore)',
  'Asia/Hong_Kong': 'HKT(Asia/Hong_Kong)',
  'Asia/Kuala_Lumpur': 'MYT(Asia/Kuala_Lumpur)',
  'Asia/Tokyo': 'JST(Asia/Tokyo)',
  'Asia/Seoul': 'KST(Asia/Seoul)',

  // Pacific Timezones
  'Pacific/Baker': 'BIT(Pacific/Baker)',
  'Pacific/Samoa': 'SST(Pacific/Samoa)',
  'Pacific/Guadalcanal': 'SBT(Pacific/Guadalcanal)',
  'Pacific/Noumea': 'NCT(Pacific/Noumea)',
  'Pacific/Efate': 'VUT(Pacific/Efate)',
  'Pacific/Auckland': 'NZST(Pacific/Auckland)',
  'Pacific/Fiji': 'FJT(Pacific/Fiji)',
  'Pacific/Funafuti': 'TVT(Pacific/Funafuti)',
  'Pacific/Tongatapu': 'TOT(Pacific/Tongatapu)',
  'Pacific/Kiritimati': 'LINT(Pacific/Kiritimati)',

  // Australian Timezones
  'Australia/Sydney': 'AEST(Australia/Sydney)',
  'Australia/Adelaide': 'ACST(Australia/Adelaide)',
  'Australia/Perth': 'CST(Asia/Shanghai)',

  // Common Timezone Abbreviations
  'EST': 'EST(America/New_York)',
  'CST': 'CST(America/Chicago)',
  'MST': 'MST(America/Denver)',
  'PST': 'PST(America/Los_Angeles)',
  'EDT': 'EDT(America/New_York)',
  'CDT': 'CDT(America/Chicago)',
  'MDT': 'MDT(America/Denver)',
  'PDT': 'PDT(America/Los_Angeles)',
  'AKST': 'AKST(America/Anchorage)',
  'AKDT': 'AKDT(America/Anchorage)',
  'HAST': 'HAST(America/Adak)',
  'HADT': 'HADT(America/Adak)',
  'HST': 'HST(Pacific/Honolulu)',
  'SST': 'SST(Pacific/Samoa)',
  'AST': 'AST(America/Puerto_Rico)',
  'WET': 'WET(Europe/London)',
  'WEST': 'WEST(Europe/London)',
  'BST': 'BST(Europe/London)',
  'CET': 'CET(Europe/Paris)',
  'CEST': 'CEST(Europe/Paris)',
  'EET': 'EET(Europe/Athens)',
  'EEST': 'EEST(Europe/Athens)',
  'IST': 'IST(Asia/Kolkata)',
  'NPT': 'NPT(Asia/Kathmandu)',
  'JST': 'JST(Asia/Tokyo)',
  'KST': 'KST(Asia/Seoul)',
  'AEST': 'AEST(Australia/Sydney)',
  'AEDT': 'AEDT(Australia/Sydney)',
  'ACST': 'ACST(Australia/Adelaide)',
  'BRT': 'BRT(America/Sao_Paulo)',
  'ART': 'ART(America/Argentina/Buenos_Aires)',
  'UYT': 'UYT(America/Montevideo)',
  'GFT': 'GFT(America/Cayenne)',
  'FNT': 'FNT(America/Noronha)',
  'AZOT': 'AZOT(Atlantic/Azores)',
  'CVT': 'CVT(Atlantic/Cape_Verde)',
  'WAT': 'WAT(Africa/Lagos)',
  'CAT': 'CAT(Africa/Harare)',
  'SAST': 'SAST(Africa/Johannesburg)',
  'EAT': 'EAT(Africa/Nairobi)',
  'MSK': 'MSK(Europe/Moscow)',
  'AMT': 'AMT(Asia/Yerevan)',
  'AZT': 'AZT(Asia/Baku)',
  'GST': 'GST(Asia/Dubai)',
  'PKT': 'PKT(Asia/Karachi)',
  'UZT': 'UZT(Asia/Tashkent)',
  'YEKT': 'YEKT(Asia/Yekaterinburg)',
  'BDT': 'BDT(Asia/Dhaka)',
  'BTT': 'BTT(Asia/Thimphu)',
  'ICT': 'ICT(Asia/Bangkok)',
  'WIB': 'WIB(Asia/Jakarta)',
  'WITA': 'WITA(Asia/Makassar)',
  'WIT': 'WIT(Asia/Jayapura)',
  'KRAT': 'KRAT(Asia/Krasnoyarsk)',
  'IRKT': 'IRKT(Asia/Irkutsk)',
  'YAKT': 'YAKT(Asia/Yakutsk)',
  'SGT': 'SGT(Asia/Singapore)',
  'HKT': 'HKT(Asia/Hong_Kong)',
  'MYT': 'MYT(Asia/Kuala_Lumpur)',
  'SBT': 'SBT(Pacific/Guadalcanal)',
  'NCT': 'NCT(Pacific/Noumea)',
  'VUT': 'VUT(Pacific/Efate)',
  'BIT': 'BIT(Pacific/Baker)',
  'NZST': 'NZST(Pacific/Auckland)',
  'NZDT': 'NZDT(Pacific/Auckland)',
  'FJT': 'FJT(Pacific/Fiji)',
  'TVT': 'TVT(Pacific/Funafuti)',
  'TOT': 'TOT(Pacific/Tongatapu)',
  'LINT': 'LINT(Pacific/Kiritimati)',
  'COT': 'COT(America/Bogota)',
  'PET': 'PET(America/Lima)',
  'VET': 'VET(America/Caracas)',
  'BOT': 'BOT(America/La_Paz)',
};

// Time unit conversion constants (matching Java TimeUnit enum)
export const TIME_UNITS = {
  NANOSECONDS: 'NANOSECONDS',
  MICROSECONDS: 'MICROSECONDS',
  MILLISECONDS: 'MILLISECONDS',
  SECONDS: 'SECONDS',
  MINUTES: 'MINUTES',
  HOURS: 'HOURS',
  DAYS: 'DAYS'
} as const;

export type TimeUnit = typeof TIME_UNITS[keyof typeof TIME_UNITS];

// Conversion factors to milliseconds
const TIME_UNIT_TO_MS: Record<TimeUnit, number> = {
  [TIME_UNITS.NANOSECONDS]: 1 / 1000000,
  [TIME_UNITS.MICROSECONDS]: 1 / 1000,
  [TIME_UNITS.MILLISECONDS]: 1,
  [TIME_UNITS.SECONDS]: 1000,
  [TIME_UNITS.MINUTES]: 60 * 1000,
  [TIME_UNITS.HOURS]: 60 * 60 * 1000,
  [TIME_UNITS.DAYS]: 24 * 60 * 60 * 1000
};

/**
 * Converts a time value from its original time unit to milliseconds
 *
 * @param timeValue - The time value to convert
 * @param timeUnit - The time unit of the input value (e.g., 'SECONDS', 'DAYS')
 * @returns The time value converted to milliseconds, or undefined if conversion fails
 */
export const convertTimeToMilliseconds = (timeValue: unknown, timeUnit?: string): number | undefined => {
  if (timeValue === null || timeValue === undefined || timeValue === '') {
    return undefined;
  }

  const num = Number(timeValue);
  if (isNaN(num)) {
    return undefined;
  }

  // If no time unit specified or unknown time unit, assume milliseconds
  if (!timeUnit || !TIME_UNIT_TO_MS[timeUnit as TimeUnit]) {
    return num;
  }

  return num * TIME_UNIT_TO_MS[timeUnit as TimeUnit];
};

/**
 * Converts a timezone to its standardized format (UTC offset or 3-letter shortcut with location)
 * @param timezone - The timezone to standardize
 * @returns The standardized timezone string
 */
export const standardizeTimezone = (timezone: string): string => {
  // If it's already a standard format, return as is
  if (timezone.startsWith('UTC') || isStandardTimezone(timezone)) {
    return timezone;
  }

  // Map from location-based timezone to standardized format
  const mapped = TIMEZONE_MAPPING[timezone];
  if (mapped) {
    return mapped;
  }

  // For unknown timezones, try to get the offset from moment
  try {
    const offset = moment.tz(timezone).format('Z');
    if (offset === 'Z') {
      return 'UTC';
    }
    return `UTC${offset}`;
  } catch {
    // Fallback to UTC if conversion fails
    return 'UTC';
  }
};

/**
 * Checks if a timezone is already in standard format
 * @param timezone - The timezone to check
 * @returns True if it's a standard timezone
 */
const isStandardTimezone = (timezone: string): boolean => {
  return STANDARD_TIMEZONES.some(tz => tz.value === timezone);
};

/**
 * Get standardized timezone options (UTC offsets and 3-letter shortcuts)
 * @returns Array of standardized timezone options
 */
export const getStandardTimezones = () => {
  return STANDARD_TIMEZONES;
};

// Get current timezone from localStorage or default
export const getCurrentTimezone = (): string => {
  const saved = localStorage.getItem('pinot_ui:timezone');
  if (saved) {
    return standardizeTimezone(saved);
  }
  return standardizeTimezone(DEFAULT_TIMEZONE);
};

// Set timezone in localStorage
export const setCurrentTimezone = (timezone: string): void => {
  const standardized = standardizeTimezone(timezone);
  localStorage.setItem('pinot_ui:timezone', standardized);
};

// Extract timezone abbreviation from standardized timezone format
const extractTimezoneAbbreviation = (standardizedTimezone: string): string => {
  // For UTC, return as is
  if (standardizedTimezone === 'UTC') {
    return 'UTC';
  }

  // For format like 'PST(America/Los_Angeles)', extract 'PST'
  const match = standardizedTimezone.match(/^([A-Z]{3,4})\(/);
  if (match) {
    return match[1];
  }

  // For UTC offsets like 'UTC-8', return as is
  if (standardizedTimezone.startsWith('UTC')) {
    return standardizedTimezone;
  }

  // Fallback
  return standardizedTimezone;
};

// Format time in the current timezone
export const formatTimeInTimezone = (time: number | string | Date, format?: string, timezone?: string): string => {
  const currentTimezone = timezone || getCurrentTimezone();
  const standardizedTimezone = standardizeTimezone(currentTimezone);

  // Convert standardized timezone to moment-compatible format
  const momentTimezone = convertToMomentTimezone(standardizedTimezone);
  const momentTime = moment(time);

  if (momentTime.isValid()) {
    const defaultFormat = 'MMMM Do YYYY, HH:mm:ss';
    const requestedFormat = format || defaultFormat;

    // Check if format includes timezone (z token)
    if (requestedFormat.includes(' z')) {
      // Extract the selected timezone abbreviation for consistency
      const selectedAbbreviation = extractTimezoneAbbreviation(standardizedTimezone);
      // Format without timezone and manually append the selected abbreviation
      const formatWithoutTz = requestedFormat.replace(' z', '');
      const formattedTime = momentTime.tz(momentTimezone).format(formatWithoutTz);
      return `${formattedTime} ${selectedAbbreviation}`;
    } else {
      // No timezone requested, format normally
      return momentTime.tz(momentTimezone).format(requestedFormat);
    }
  }

  return 'Invalid time';
};

/**
 * Converts standardized timezone format to moment-compatible timezone
 * @param standardizedTimezone - Timezone in standardized format (e.g., 'EST(America/New_York)', 'UTC-5')
 * @returns Moment-compatible timezone string
 */
const convertToMomentTimezone = (standardizedTimezone: string): string => {
  if (standardizedTimezone === 'UTC') {
    return 'UTC';
  }

  // Handle 3-letter shortcuts with location information
  const shortcutWithLocationMatch = standardizedTimezone.match(/^([A-Z]{3,4})\(([^)]+)\)$/);
  if (shortcutWithLocationMatch) {
    const [, shortcut, location] = shortcutWithLocationMatch;
    return location; // Use the location part for moment
  }

  // Handle 3-letter shortcuts without location (legacy support)
  const shortcutMapping: Record<string, string> = {
    'GMT': 'UTC',
    'BIT': 'Pacific/Baker',
    'SST': 'Pacific/Samoa',
    'HST': 'Pacific/Honolulu',
    'HAST': 'America/Adak',
    'HADT': 'America/Adak',
    'AKDT': 'America/Anchorage',
    'AKST': 'America/Anchorage',
    'PST': 'America/Los_Angeles',
    'PDT': 'America/Los_Angeles',
    'MST': 'America/Denver',
    'MDT': 'America/Denver',
    'CST': 'America/Chicago',
    'CDT': 'America/Chicago',
    'EST': 'America/New_York',
    'EDT': 'America/New_York',
    'AST': 'America/Puerto_Rico',
    'COT': 'America/Bogota',
    'PET': 'America/Lima',
    'VET': 'America/Caracas',
    'BOT': 'America/La_Paz',
    'ART': 'America/Argentina/Buenos_Aires',
    'BRT': 'America/Sao_Paulo',
    'UYT': 'America/Montevideo',
    'GFT': 'America/Cayenne',
    'FNT': 'America/Noronha',
    'AZOT': 'Atlantic/Azores',
    'CVT': 'Atlantic/Cape_Verde',
    'WET': 'Europe/London',
    'WEST': 'Europe/London',
    'BST': 'Europe/London',
    'WAT': 'Africa/Lagos',
    'CET': 'Europe/Paris',
    'CEST': 'Europe/Paris',
    'CAT': 'Africa/Harare',
    'EET': 'Europe/Athens',
    'EEST': 'Europe/Athens',
    'SAST': 'Africa/Johannesburg',
    'EAT': 'Africa/Nairobi',
    'MSK': 'Europe/Moscow',
    'IST': 'Asia/Kolkata',
    'NPT': 'Asia/Kathmandu',
    'BDT': 'Asia/Dhaka',
    'BTT': 'Asia/Thimphu',
    'AMT': 'Asia/Yerevan',
    'AZT': 'Asia/Baku',
    'GST': 'Asia/Dubai',
    'PKT': 'Asia/Karachi',
    'UZT': 'Asia/Tashkent',
    'YEKT': 'Asia/Yekaterinburg',
    'ICT': 'Asia/Bangkok',
    'WIB': 'Asia/Jakarta',
    'WITA': 'Asia/Makassar',
    'WIT': 'Asia/Jayapura',
    'KRAT': 'Asia/Krasnoyarsk',
    'IRKT': 'Asia/Irkutsk',
    'YAKT': 'Asia/Yakutsk',
    'SGT': 'Asia/Singapore',
    'HKT': 'Asia/Hong_Kong',
    'MYT': 'Asia/Kuala_Lumpur',
    'JST': 'Asia/Tokyo',
    'KST': 'Asia/Seoul',
    'ACST': 'Australia/Adelaide',
    'AEST': 'Australia/Sydney',
    'AEDT': 'Australia/Sydney',
    'SBT': 'Pacific/Guadalcanal',
    'NCT': 'Pacific/Noumea',
    'VUT': 'Pacific/Efate',
    'NZST': 'Pacific/Auckland',
    'NZDT': 'Pacific/Auckland',
    'FJT': 'Pacific/Fiji',
    'TVT': 'Pacific/Funafuti',
    'TOT': 'Pacific/Tongatapu',
    'LINT': 'Pacific/Kiritimati',
  };

  const mapped = shortcutMapping[standardizedTimezone];
  if (mapped) {
    return mapped;
  }

  // Handle UTC offset format
  if (standardizedTimezone.startsWith('UTC')) {
    const offsetMatch = standardizedTimezone.match(/^UTC([+-]\d+(?::\d+)?)$/);
    if (offsetMatch) {
      const offset = offsetMatch[1];
      // Convert to moment format
      if (offset.includes(':')) {
        return `Etc/GMT${offset.startsWith('+') ? '-' : '+'}${offset.substring(1)}`;
      } else {
        const hours = parseInt(offset);
        const sign = hours >= 0 ? '-' : '+';
        const absHours = Math.abs(hours);
        return `Etc/GMT${sign}${absHours}`;
      }
    }
  }

  return 'UTC';
};

// Convert time to a specific timezone
export const convertTimeToTimezone = (time: number | string | Date, targetTimezone: string): moment.Moment => {
  const standardizedTimezone = standardizeTimezone(targetTimezone);
  const momentTimezone = convertToMomentTimezone(standardizedTimezone);
  return moment(time).tz(momentTimezone);
};

// Get timezone offset string
export const getTimezoneOffset = (timezone: string): string => {
  const standardizedTimezone = standardizeTimezone(timezone);
  const momentTimezone = convertToMomentTimezone(standardizedTimezone);
  return moment.tz(momentTimezone).format('Z');
};

// Get timezone display name
export const getTimezoneDisplayName = (timezone: string): string => {
  const standardizedTimezone = standardizeTimezone(timezone);
  const offset = getTimezoneOffset(standardizedTimezone);
  return `${standardizedTimezone} (${offset})`;
};

/**
 * Checks if the provided timezone string is a valid standardized timezone.
 *
 * @param timezone - The timezone string to validate
 * @returns {boolean} True if the timezone is valid, false otherwise.
 */
export const isValidTimezone = (timezone: string): boolean => {
  const standardized = standardizeTimezone(timezone);
  return STANDARD_TIMEZONES.some(tz => tz.value === standardized);
};

// Legacy function for backward compatibility - now returns standardized timezones
export const getAllTimezones = () => {
  return getStandardTimezones();
};

// Legacy function for backward compatibility
export const COMMON_TIMEZONES = STANDARD_TIMEZONES;
