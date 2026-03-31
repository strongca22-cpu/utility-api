/**
 * Hook to lazy-load county rates for QA comparison.
 * Only fetches county_rates.json when in QA mode and a utility is selected.
 * Caches the full file after first load.
 */

import { useState, useEffect, useRef } from "react";
import { useDashboard } from "../contexts/DashboardContext";

const BASE_URL = import.meta.env.BASE_URL || "/";

export function useCountyRates(county, state) {
  const { appMode } = useDashboard();
  const cacheRef = useRef(null);
  const [rates, setRates] = useState(null);

  useEffect(() => {
    if (appMode !== "qa" || !county || !state) {
      setRates(null);
      return;
    }

    const key = `${state}:${county}`;

    // If already cached, return immediately
    if (cacheRef.current) {
      setRates(cacheRef.current[key] || []);
      return;
    }

    // Fetch and cache
    let cancelled = false;
    fetch(`${BASE_URL}data/county_rates.json`)
      .then((r) => r.json())
      .then((data) => {
        if (cancelled) return;
        cacheRef.current = data;
        setRates(data[key] || []);
      })
      .catch(() => {
        if (!cancelled) setRates([]);
      });

    return () => { cancelled = true; };
  }, [county, state, appMode]);

  return rates;
}
