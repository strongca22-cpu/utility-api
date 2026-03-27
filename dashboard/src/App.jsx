/**
 * UAPI Rate Explorer — Main application shell.
 *
 * Layout uses CSS grid with explicit regions so MapLibre's absolute
 * positioning doesn't escape its container.
 */

import { useState, useCallback, useRef, useEffect } from "react";
import { useMapData } from "./hooks/useMapData";
import { useSelectedUtility } from "./hooks/useSelectedUtility";
import { useDynamicStats } from "./hooks/useDynamicStats";
import { DEFAULT_BILL_RAMP } from "./utils/colors";
import Map from "./components/Map";
import DetailPanel from "./components/DetailPanel";
import CoverageBar from "./components/CoverageBar";
import LayerToggle from "./components/LayerToggle";
import SettingsPanel from "./components/SettingsPanel";
import BillLegend from "./components/BillLegend";
import DevTools from "./components/DevTools";

const DEFAULT_SETTINGS = {
  fillOpacity: 0.6,
  showFree: true,
  showPremium: true,
  showReference: true,
  showNoData: true,
  showOutlines: true,
};

export default function App() {
  const { geojson, stats: staticStats, loading, error } = useMapData();
  const { selected, select, deselect } = useSelectedUtility();
  const [layerMode, setLayerMode] = useState("coverage");
  const [mapSettings, setMapSettings] = useState(DEFAULT_SETTINGS);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [billRamp, setBillRamp] = useState(DEFAULT_BILL_RAMP);
  const [devToolsOpen, setDevToolsOpen] = useState(false);
  const mapRef = useRef(null);

  // Dynamic stats based on current tier filter
  const dynamicStats = useDynamicStats(geojson, mapSettings);

  // Ctrl+Shift+D toggles dev tools
  useEffect(() => {
    function handleKey(e) {
      if (e.ctrlKey && e.shiftKey && e.key === "D") {
        e.preventDefault();
        setDevToolsOpen((v) => !v);
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, []);

  const handleFeatureClick = useCallback(
    (properties, feature) => {
      select(properties);

      // Zoom to feature centroid
      if (feature?.geometry) {
        const coords = flattenCoords(feature.geometry);
        if (coords.length > 0) {
          const centroid = getCentroid(coords);
          mapRef.current?.flyTo(centroid[0], centroid[1], 10);
        }
      }
    },
    [select]
  );

  if (error) {
    return (
      <div className="flex h-full items-center justify-center bg-slate-900 text-red-400">
        <div className="text-center">
          <p className="text-lg font-semibold">Failed to load data</p>
          <p className="text-sm text-slate-500 mt-1">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div
      style={{
        display: "grid",
        gridTemplateRows: "auto 1fr auto",
        gridTemplateColumns: selected ? "1fr 320px" : "1fr",
        height: "100vh",
        width: "100vw",
        overflow: "hidden",
      }}
    >
      {/* Top bar — spans full width */}
      <div
        className="flex items-center justify-between border-b border-slate-700 bg-slate-800 px-4 py-2"
        style={{ gridColumn: "1 / -1" }}
      >
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold text-slate-100">
            UAPI Rate Explorer
          </h1>
          {/* Dev tools toggle (wrench icon) */}
          <button
            onClick={() => setDevToolsOpen(!devToolsOpen)}
            className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${
              devToolsOpen
                ? "bg-amber-600/20 text-amber-400"
                : "text-slate-600 hover:text-slate-400"
            }`}
            aria-label="Toggle dev tools"
            title="Dev Tools (Ctrl+Shift+D)"
          >
            <svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor">
              <path d="M11.7 1.3a1 1 0 011.4 0l1.6 1.6a1 1 0 010 1.4L13.4 5.6l-3-3 1.3-1.3zM2 11.5V14.5h3l7.4-7.4-3-3L2 11.5z" />
            </svg>
          </button>
        </div>
        <div className="flex items-center gap-3">
          <LayerToggle mode={layerMode} onChange={setLayerMode} />
          {/* Settings gear */}
          <div className="relative">
            <button
              onClick={() => setSettingsOpen(!settingsOpen)}
              className={`w-8 h-8 flex items-center justify-center rounded transition-colors ${
                settingsOpen
                  ? "bg-slate-600 text-slate-100"
                  : "text-slate-400 hover:text-slate-200 hover:bg-slate-700"
              }`}
              aria-label="Map settings"
            >
              <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 10a2 2 0 100-4 2 2 0 000 4z" />
                <path fillRule="evenodd" d="M6.343 1.2a1 1 0 01.98-.8h1.354a1 1 0 01.98.8l.175 1.046a5.5 5.5 0 011.108.64l.99-.363a1 1 0 011.14.392l.677 1.173a1 1 0 01-.16 1.191l-.816.684a5.5 5.5 0 010 1.28l.816.683a1 1 0 01.16 1.191l-.677 1.173a1 1 0 01-1.14.392l-.99-.363a5.5 5.5 0 01-1.108.64l-.175 1.046a1 1 0 01-.98.8H7.323a1 1 0 01-.98-.8l-.175-1.046a5.5 5.5 0 01-1.108-.64l-.99.363a1 1 0 01-1.14-.392l-.677-1.173a1 1 0 01.16-1.191l.816-.684a5.5 5.5 0 010-1.28l-.816-.683a1 1 0 01-.16-1.191L2.93 3.915a1 1 0 011.14-.392l.99.363a5.5 5.5 0 011.108-.64L6.343 1.2zM8 11a3 3 0 100-6 3 3 0 000 6z" clipRule="evenodd" />
              </svg>
            </button>
            {settingsOpen && (
              <SettingsPanel
                settings={mapSettings}
                onChange={setMapSettings}
                onClose={() => setSettingsOpen(false)}
              />
            )}
          </div>
        </div>
      </div>

      {/* Map — row 2, col 1 */}
      <div style={{ position: "relative", overflow: "hidden" }}>
        {loading ? (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <div className="inline-block h-8 w-8 animate-spin rounded-full border-4 border-slate-600 border-t-blue-500" />
              <p className="mt-3 text-sm text-slate-400">
                Loading service areas...
              </p>
            </div>
          </div>
        ) : (
          <>
            <Map
              ref={mapRef}
              geojson={geojson}
              layerMode={layerMode}
              billRamp={billRamp}
              mapSettings={mapSettings}
              onFeatureClick={handleFeatureClick}
            />
            <BillLegend rampKey={billRamp} visible={layerMode === "bill"} />
            {devToolsOpen && (
              <DevTools
                billRamp={billRamp}
                onBillRampChange={setBillRamp}
                onClose={() => setDevToolsOpen(false)}
              />
            )}
          </>
        )}
      </div>

      {/* Detail panel — row 2, col 2 (only when selected) */}
      {selected && (
        <DetailPanel utility={selected} onClose={deselect} />
      )}

      {/* Bottom coverage bar — spans full width */}
      <div style={{ gridColumn: "1 / -1" }}>
        <CoverageBar stats={dynamicStats} staticStats={staticStats} />
      </div>
    </div>
  );
}

/**
 * Flatten all coordinates from a GeoJSON geometry into a flat array of [lng, lat].
 */
function flattenCoords(geometry) {
  if (!geometry || !geometry.coordinates) return [];
  const type = geometry.type;
  if (type === "Polygon") {
    return geometry.coordinates[0];
  }
  if (type === "MultiPolygon") {
    return geometry.coordinates.flatMap((poly) => poly[0]);
  }
  return [];
}

/**
 * Compute centroid of a set of [lng, lat] coordinates.
 */
function getCentroid(coords) {
  let sumLng = 0, sumLat = 0;
  for (const c of coords) {
    sumLng += c[0];
    sumLat += c[1];
  }
  return [sumLng / coords.length, sumLat / coords.length];
}
