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

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { getCurrentTimezone, setCurrentTimezone, DEFAULT_TIMEZONE } from '../utils/TimezoneUtils';

interface TimezoneContextType {
  currentTimezone: string;
  setTimezone: (timezone: string) => void;
}

const TimezoneContext = createContext<TimezoneContextType | undefined>(undefined);

interface TimezoneProviderProps {
  children: ReactNode;
}

export const TimezoneProvider: React.FC<TimezoneProviderProps> = ({ children }) => {
  const [currentTimezone, setCurrentTimezoneState] = useState<string>(DEFAULT_TIMEZONE);

  useEffect(() => {
    // Initialize timezone from localStorage
    const savedTimezone = getCurrentTimezone();
    setCurrentTimezoneState(savedTimezone);
  }, []);

  const setTimezone = (timezone: string) => {
    setCurrentTimezoneState(timezone);
    setCurrentTimezone(timezone);
  };

  const value: TimezoneContextType = {
    currentTimezone,
    setTimezone,
  };

  return (
    <TimezoneContext.Provider value={value}>
      {children}
    </TimezoneContext.Provider>
  );
};

export const useTimezone = (): TimezoneContextType => {
  const context = useContext(TimezoneContext);
  if (context === undefined) {
    throw new Error('useTimezone must be used within a TimezoneProvider');
  }
  return context;
};
