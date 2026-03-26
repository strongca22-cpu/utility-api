/**
 * Main map component — MapLibre GL JS with CWS polygon layers.
 *
 * Renders two layers:
 *   1. cws-fill: polygon fill (colored by coverage or bill amount)
 *   2. cws-outline: polygon outline (visible at higher zoom levels)
 *
 * Handles hover (tooltip) and click (detail panel) interactions.
 */

import { useRef, useEffect, useState } from "react";
import maplibregl from "maplibre-gl";
import Tooltip from "./Tooltip";
import { coverageFillExpression, billFillExpression } from "../utils/colors";

const SOURCE_ID = "cws";
const FILL_LAYER = "cws-fill";
const OUTLINE_LAYER = "cws-outline";

const INITIAL_CENTER = [-98.5, 39.5];
const INITIAL_ZOOM = 4;

export default function Map({ geojson, layerMode, onFeatureClick }) {
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const hoveredIdRef = useRef(null);
  const [tooltip, setTooltip] = useState(null);

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

    return () => {
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
          "fill-opacity": [
            "case",
            ["boolean", ["feature-state", "hover"], false],
            0.85,
            ["==", ["get", "has_rate_data"], true],
            0.6,
            ["==", ["get", "has_reference_only"], true],
            0.5,
            0.15,
          ],
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

      // Attach interactions after layers exist
      attachHover(map);
      attachClick(map);
    }

    if (map.isStyleLoaded()) {
      addLayers();
    } else {
      map.on("load", addLayers);
    }
  }, [geojson]);

  // Update fill color when layer mode changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.getLayer(FILL_LAYER)) return;
    const expr = layerMode === "bill" ? billFillExpression() : coverageFillExpression();
    map.setPaintProperty(FILL_LAYER, "fill-color", expr);
  }, [layerMode]);

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
        onClickRef.current(e.features[0].properties);
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
      />
    </div>
  );
}
