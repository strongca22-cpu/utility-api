/**
 * Main map component — MapLibre GL JS with CWS polygon layers.
 *
 * Renders two layers:
 *   1. cws-fill: polygon fill (colored by coverage or bill amount)
 *   2. cws-outline: polygon outline (visible at higher zoom levels)
 *
 * Handles hover (tooltip) and click (detail panel + zoom-to-feature).
 * Supports settings: fill opacity, reference/no-data visibility, outlines.
 */

import { useRef, useEffect, useState, useImperativeHandle, forwardRef } from "react";
import maplibregl from "maplibre-gl";
import Tooltip from "./Tooltip";
import { coverageFillExpression, billFillExpression } from "../utils/colors";

const SOURCE_ID = "cws";
const FILL_LAYER = "cws-fill";
const OUTLINE_LAYER = "cws-outline";
const STATES_SOURCE = "us-states";
const STATES_LAYER = "state-boundaries";
const COUNTIES_SOURCE = "us-counties";
const COUNTIES_LAYER = "county-boundaries";

const BASE_URL = import.meta.env.BASE_URL || "/";

const INITIAL_CENTER = [-98.5, 39.5];
const INITIAL_ZOOM = 4;

const Map = forwardRef(function Map({ geojson, layerMode, billRamp, mapSettings, onFeatureClick }, ref) {
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const hoveredIdRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);
  const [containerSize, setContainerSize] = useState({ w: 0, h: 0 });

  // Expose flyTo method to parent
  useImperativeHandle(ref, () => ({
    flyTo(lng, lat, zoom) {
      const map = mapRef.current;
      if (map) {
        map.flyTo({ center: [lng, lat], zoom: zoom || 10, duration: 800 });
      }
    },
  }));

  // Initialize map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: {
        version: 8,
        sources: {
          "osm-tiles": {
            type: "raster",
            tiles: [
              "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
              "https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
              "https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
            ],
            tileSize: 256,
            attribution:
              '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
          },
        },
        layers: [
          {
            id: "background",
            type: "background",
            paint: { "background-color": "#0f172a" },
          },
          {
            id: "osm-tiles",
            type: "raster",
            source: "osm-tiles",
            minzoom: 0,
            maxzoom: 19,
          },
        ],
      },
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM,
      maxZoom: 14,
    });

    map.addControl(new maplibregl.NavigationControl(), "top-left");
    mapRef.current = map;

    // Track container size for tooltip edge detection
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerSize({
          w: entry.contentRect.width,
          h: entry.contentRect.height,
        });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // Add GeoJSON source + layers when data is available
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !geojson) return;

    function addLayers() {
      if (map.getSource(SOURCE_ID)) return;

      map.addSource(SOURCE_ID, {
        type: "geojson",
        data: geojson,
        promoteId: "pwsid",
      });

      map.addLayer({
        id: FILL_LAYER,
        type: "fill",
        source: SOURCE_ID,
        paint: {
          "fill-color": coverageFillExpression(),
          "fill-opacity": buildOpacityExpression(mapSettings),
        },
      });

      map.addLayer({
        id: OUTLINE_LAYER,
        type: "line",
        source: SOURCE_ID,
        paint: {
          "line-color": "#475569",
          "line-width": [
            "interpolate", ["linear"], ["zoom"],
            6, 0,
            8, 0.5,
            12, 1,
          ],
        },
        minzoom: 6,
      });

      // Boundary layers (loaded from static GeoJSON, hidden by default)
      fetch(`${BASE_URL}data/us_states_simplified.geojson`)
        .then((r) => r.json())
        .then((data) => {
          if (map.getSource(STATES_SOURCE)) return;
          map.addSource(STATES_SOURCE, { type: "geojson", data });
          map.addLayer({
            id: STATES_LAYER,
            type: "line",
            source: STATES_SOURCE,
            paint: { "line-color": "#94a3b8", "line-width": 1.5 },
            layout: { visibility: "none" },
          });
        })
        .catch(() => {}); // Boundary files optional

      fetch(`${BASE_URL}data/us_counties_simplified.geojson`)
        .then((r) => r.json())
        .then((data) => {
          if (map.getSource(COUNTIES_SOURCE)) return;
          map.addSource(COUNTIES_SOURCE, { type: "geojson", data });
          map.addLayer({
            id: COUNTIES_LAYER,
            type: "line",
            source: COUNTIES_SOURCE,
            paint: { "line-color": "#64748b", "line-width": 0.5 },
            layout: { visibility: "none" },
            minzoom: 6,
          });
        })
        .catch(() => {});

      attachHover(map);
      attachClick(map);
    }

    if (map.isStyleLoaded()) {
      addLayers();
    } else {
      map.on("load", addLayers);
    }
  }, [geojson]);

  // Update fill color when layer mode or bill ramp changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.getLayer(FILL_LAYER)) return;
    const expr = layerMode === "bill" ? billFillExpression(billRamp) : coverageFillExpression();
    map.setPaintProperty(FILL_LAYER, "fill-color", expr);
  }, [layerMode, billRamp]);

  // Update opacity + visibility when settings change
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.getLayer(FILL_LAYER)) return;

    map.setPaintProperty(FILL_LAYER, "fill-opacity", buildOpacityExpression(mapSettings));

    // Outline visibility
    if (map.getLayer(OUTLINE_LAYER)) {
      map.setLayoutProperty(
        OUTLINE_LAYER,
        "visibility",
        mapSettings.showOutlines ? "visible" : "none"
      );
    }

    // Boundary layer visibility
    if (map.getLayer(STATES_LAYER)) {
      map.setLayoutProperty(
        STATES_LAYER,
        "visibility",
        mapSettings.showStateBoundaries ? "visible" : "none"
      );
    }
    if (map.getLayer(COUNTIES_LAYER)) {
      map.setLayoutProperty(
        COUNTIES_LAYER,
        "visibility",
        mapSettings.showCountyBoundaries ? "visible" : "none"
      );
    }
  }, [mapSettings]);

  // Store onFeatureClick in a ref so the click handler always has current value
  const onClickRef = useRef(onFeatureClick);
  useEffect(() => {
    onClickRef.current = onFeatureClick;
  }, [onFeatureClick]);

  function attachHover(map) {
    map.on("mousemove", FILL_LAYER, (e) => {
      if (hoveredIdRef.current !== null) {
        map.setFeatureState(
          { source: SOURCE_ID, id: hoveredIdRef.current },
          { hover: false }
        );
      }
      if (e.features.length > 0) {
        const feat = e.features[0];
        hoveredIdRef.current = feat.id;
        map.setFeatureState(
          { source: SOURCE_ID, id: feat.id },
          { hover: true }
        );
        map.getCanvas().style.cursor = "pointer";
        setTooltip({ feature: feat, x: e.point.x, y: e.point.y });
      }
    });

    map.on("mouseleave", FILL_LAYER, () => {
      if (hoveredIdRef.current !== null) {
        map.setFeatureState(
          { source: SOURCE_ID, id: hoveredIdRef.current },
          { hover: false }
        );
        hoveredIdRef.current = null;
      }
      map.getCanvas().style.cursor = "";
      setTooltip(null);
    });
  }

  function attachClick(map) {
    map.on("click", FILL_LAYER, (e) => {
      if (e.features.length > 0) {
        const feat = e.features[0];
        onClickRef.current(feat.properties, feat);
      }
    });
  }

  return (
    <div style={{ position: "absolute", top: 0, left: 0, right: 0, bottom: 0 }}>
      <div
        ref={containerRef}
        style={{ width: "100%", height: "100%" }}
      />
      <Tooltip
        feature={tooltip?.feature}
        x={tooltip?.x || 0}
        y={tooltip?.y || 0}
        containerWidth={containerSize.w}
        containerHeight={containerSize.h}
      />
    </div>
  );
});

/**
 * Build a Maplibre fill-opacity expression based on current settings.
 * Handles: hover boost, per-tier visibility, no-data hiding.
 *
 * data_tier values: "free", "premium", "reference", null (no data)
 */
function buildOpacityExpression(settings) {
  const base = settings?.fillOpacity ?? 0.6;
  const showFree = settings?.showFree ?? true;
  const showPremium = settings?.showPremium ?? true;
  const showRef = settings?.showReference ?? true;
  const showNoData = settings?.showNoData ?? true;

  return [
    "case",
    // Hover: always boost (but only if the tier is visible)
    ["all",
      ["boolean", ["feature-state", "hover"], false],
      ["any",
        ["all", ["==", ["get", "data_tier"], "free"], ["literal", showFree]],
        ["all", ["==", ["get", "data_tier"], "premium"], ["literal", showPremium]],
        ["all", ["==", ["get", "data_tier"], "reference"], ["literal", showRef]],
        ["all", ["!", ["has", "data_tier"]], ["literal", showNoData]],
      ],
    ],
    Math.min(base + 0.25, 1),
    // Free tier
    ["==", ["get", "data_tier"], "free"],
    showFree ? base : 0,
    // Premium tier
    ["==", ["get", "data_tier"], "premium"],
    showPremium ? base : 0,
    // Reference tier
    ["==", ["get", "data_tier"], "reference"],
    showRef ? base * 0.8 : 0,
    // No data (data_tier is null)
    showNoData ? base * 0.25 : 0,
  ];
}

export default Map;
