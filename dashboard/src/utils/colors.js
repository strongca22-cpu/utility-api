/**
 * Color constants and ramp functions for the choropleth layers.
 *
 * Coverage mode: green (free/gov), blue (premium), amber (reference), gray (no data)
 * Bill mode:     selectable neutral sequential ramp (no good/bad connotation)
 */

// --- Tier colors (used in coverage mode, settings panel, coverage bar) ---
export const TIER_COLORS = {
  free: "#059669",            // emerald-600 — government/survey data
  premium: "#2563eb",         // blue-600 — LLM-scraped (proprietary)
  reference: "#f59e0b",       // amber-500 — Duke NIEPS (internal only)
  noData: "#e5e7eb",          // gray-200
};

// --- Bill color ramps (neutral sequential, no good/bad valence) ---
// Each ramp: array of [billAmount, hexColor] pairs for Maplibre interpolate
export const BILL_RAMPS = {
  teal: {
    name: "Teal",
    description: "ColorBrewer Blues — light steel to dark navy",
    stops: [
      [0, "#C6DBEF"],
      [20, "#9ECAE1"],
      [35, "#6BAED6"],
      [50, "#3182BD"],
      [75, "#1A6D8E"],
      [100, "#08306B"],
    ],
  },
  violet: {
    name: "Violet → Indigo",
    description: "Plasma-inspired — dusty rose to deep indigo",
    stops: [
      [0, "#DAAFC6"],
      [20, "#C488BE"],
      [35, "#9B72AA"],
      [50, "#7B5C9E"],
      [75, "#584B96"],
      [100, "#2D1E5B"],
    ],
  },
  earth: {
    name: "Earth",
    description: "CARTO Earth — pale gold to dark mahogany",
    stops: [
      [0, "#FEDD84"],
      [20, "#F2B950"],
      [35, "#E8932E"],
      [50, "#D66B27"],
      [75, "#B03A2E"],
      [100, "#6C2116"],
    ],
  },
};

export const DEFAULT_BILL_RAMP = "violet";

// --- UI chrome colors ---
export const CHROME = {
  sidebarBg: "#1e293b",   // slate-800
  sidebarText: "#f1f5f9", // slate-100
  panelBg: "#0f172a",     // slate-900
  accent: "#3b82f6",      // blue-500
  bottomBar: "#1e293b",   // slate-800
};

/**
 * Build a Maplibre fill-color expression for coverage mode.
 * Colors by data_tier: free (green), premium (blue), reference (amber), no data (gray).
 */
export function coverageFillExpression() {
  return [
    "match",
    ["get", "data_tier"],
    "free", TIER_COLORS.free,
    "premium", TIER_COLORS.premium,
    "reference", TIER_COLORS.reference,
    TIER_COLORS.noData,
  ];
}

/**
 * Build a Maplibre fill-color expression for bill-at-10CCF mode.
 * Uses the specified ramp key, falling back to gray for nulls.
 */
export function billFillExpression(rampKey) {
  const ramp = BILL_RAMPS[rampKey] || BILL_RAMPS[DEFAULT_BILL_RAMP];
  return [
    "case",
    ["==", ["get", "bill_10ccf"], null],
    TIER_COLORS.noData,
    [
      "interpolate",
      ["linear"],
      ["get", "bill_10ccf"],
      ...ramp.stops.flat(),
    ],
  ];
}
