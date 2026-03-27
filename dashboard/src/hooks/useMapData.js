/**
 * Hook to load GeoJSON data and coverage stats for the map.
 *
 * Fetches from /data/cws_rates.geojson and /data/coverage_stats.json
 * served by Vite's public directory.
 */

import { useState, useEffect } from "react";

export function useMapData() {
  const [geojson, setGeojson] = useState(null);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const base = import.meta.env.BASE_URL || "/";
        const [geoRes, statsRes] = await Promise.all([
          fetch(`${base}data/cws_rates.geojson`),
          fetch(`${base}data/coverage_stats.json`),
        ]);

        if (!geoRes.ok) throw new Error(`GeoJSON fetch failed: ${geoRes.status}`);
        if (!statsRes.ok) throw new Error(`Stats fetch failed: ${statsRes.status}`);

        const [geoData, statsData] = await Promise.all([
          geoRes.json(),
          statsRes.json(),
        ]);

        if (!cancelled) {
          setGeojson(geoData);
          setStats(statsData);
          setLoading(false);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message);
          setLoading(false);
        }
      }
    }

    load();
    return () => { cancelled = true; };
  }, []);

  return { geojson, stats, loading, error };
}
