/**
 * UAPI Dashboard — Main application shell.
 *
 * Layout: left sidebar (260px) + map (flex) + optional detail panel (320px).
 * Uses DashboardContext for shared state across all components.
 */

import { useCallback, useRef, useEffect } from "react";
import { DashboardProvider, useDashboard, useDashboardDispatch } from "./contexts/DashboardContext";
import { useMapData } from "./hooks/useMapData";
import { useDynamicStats } from "./hooks/useDynamicStats";
import Map from "./components/Map";
import DetailPanel from "./components/DetailPanel";
import CoverageBar from "./components/CoverageBar";
import BillLegend from "./components/BillLegend";
import DevTools from "./components/DevTools";
import Sidebar from "./components/Sidebar";

export default function App() {
  return (
    <DashboardProvider>
      <AppInner />
    </DashboardProvider>
  );
}

function AppInner() {
  const state = useDashboard();
  const dispatch = useDashboardDispatch();
  const { geojson, stats: staticStats, loading, error } = useMapData();
  const mapRef = useRef(null);

  // Build mapSettings from context for backward compat with hooks
  const mapSettings = {
    fillOpacity: state.fillOpacity,
    showFree: state.showFree,
    showPremium: state.showPremium,
    showReference: state.showReference,
    showNoData: state.showNoData,
    showOutlines: state.showOutlines,
    showStateBoundaries: state.showStateBoundaries,
    showCountyBoundaries: state.showCountyBoundaries,
  };

  const dynamicStats = useDynamicStats(geojson, mapSettings);

  // Ctrl+Shift+D toggles dev tools
  useEffect(() => {
    function handleKey(e) {
      if (e.ctrlKey && e.shiftKey && e.key === "D") {
        e.preventDefault();
        dispatch({ type: "TOGGLE_DEVTOOLS" });
      }
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [dispatch]);

  const handleFeatureClick = useCallback(
    (properties, feature) => {
      dispatch({ type: "SELECT_UTILITY", payload: properties });

      // Zoom to feature centroid
      if (feature?.geometry) {
        const coords = flattenCoords(feature.geometry);
        if (coords.length > 0) {
          const centroid = getCentroid(coords);
          mapRef.current?.flyTo(centroid[0], centroid[1], 10);
        }
      }
    },
    [dispatch]
  );

  const handleDeselect = useCallback(() => {
    dispatch({ type: "DESELECT" });
  }, [dispatch]);

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
        gridTemplateRows: "1fr auto",
        gridTemplateColumns: [
          state.sidebarOpen ? "260px" : "0px",
          "1fr",
          state.selected ? "320px" : "",
        ].filter(Boolean).join(" "),
        height: "100vh",
        width: "100vw",
        overflow: "hidden",
      }}
    >
      {/* Left sidebar — row 1, col 1 (collapsible) */}
      {state.sidebarOpen && <Sidebar dynamicStats={dynamicStats} />}

      {/* Sidebar toggle — floats on map edge when collapsed */}
      {!state.sidebarOpen && (
        <button
          onClick={() => dispatch({ type: "TOGGLE_SIDEBAR" })}
          className="absolute top-3 left-3 z-30 w-8 h-8 flex items-center justify-center rounded-md bg-slate-800/90 border border-slate-600 text-slate-400 hover:text-slate-100 hover:bg-slate-700 transition-colors shadow-lg"
          aria-label="Open sidebar"
          title="Open sidebar"
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M1 3h12M1 7h12M1 11h12" />
          </svg>
        </button>
      )}

      {/* Map — row 1, col 2 */}
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
              layerMode={state.layerMode}
              billRamp={state.billRamp}
              mapSettings={mapSettings}
              onFeatureClick={handleFeatureClick}
            />
            <BillLegend rampKey={state.billRamp} visible={state.layerMode === "bill"} />
            {state.devToolsOpen && (
              <DevTools
                billRamp={state.billRamp}
                onBillRampChange={(ramp) => dispatch({ type: "SET_BILL_RAMP", payload: ramp })}
                onClose={() => dispatch({ type: "TOGGLE_DEVTOOLS" })}
              />
            )}
          </>
        )}
      </div>

      {/* Detail panel — row 1, col 3 (only when selected) */}
      {state.selected && (
        <DetailPanel utility={state.selected} onClose={handleDeselect} />
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
