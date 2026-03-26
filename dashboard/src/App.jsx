/**
 * UAPI Rate Explorer — Main application shell.
 *
 * Layout uses CSS grid with explicit regions so MapLibre's absolute
 * positioning doesn't escape its container.
 */

import { useState, useCallback } from "react";
import { useMapData } from "./hooks/useMapData";
import { useSelectedUtility } from "./hooks/useSelectedUtility";
import Map from "./components/Map";
import DetailPanel from "./components/DetailPanel";
import CoverageBar from "./components/CoverageBar";
import LayerToggle from "./components/LayerToggle";

export default function App() {
  const { geojson, stats, loading, error } = useMapData();
  const { selected, select, deselect } = useSelectedUtility();
  const [layerMode, setLayerMode] = useState("coverage");

  const handleFeatureClick = useCallback(
    (properties) => {
      select(properties);
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
        <h1 className="text-lg font-semibold text-slate-100">
          UAPI Rate Explorer
        </h1>
        <LayerToggle mode={layerMode} onChange={setLayerMode} />
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
          <Map
            geojson={geojson}
            layerMode={layerMode}
            onFeatureClick={handleFeatureClick}
          />
        )}
      </div>

      {/* Detail panel — row 2, col 2 (only when selected) */}
      {selected && (
        <DetailPanel utility={selected} onClose={deselect} />
      )}

      {/* Bottom coverage bar — spans full width */}
      <div style={{ gridColumn: "1 / -1" }}>
        <CoverageBar stats={stats} />
      </div>
    </div>
  );
}
