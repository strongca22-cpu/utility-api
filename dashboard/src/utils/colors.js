/**
 * Color constants and ramp functions for the choropleth layers.
 *
 * Coverage mode:  blue (has data), amber (reference only), gray (no data)
 * Bill mode:      green→yellow→orange→red graduated by bill_10ccf
 */

// --- Coverage mode colors ---
export const COVERAGE_COLORS = {
  hasData: "#2563eb",       // blue-600
  referenceOnly: "#f59e0b", // amber-500
  noData: "#e5e7eb",        // gray-200
};

// --- Bill mode color stops (bill_10ccf in $/month) ---
// Maplibre interpolate-hcl expression expects [value, color] pairs
export const BILL_COLOR_STOPS = [
  [0, "#059669"],    // emerald-600 (cheapest)
  [20, "#059669"],
  [25, "#34d399"],   // emerald-400
  [35, "#fbbf24"],   // amber-400 (median range)
  [50, "#f97316"],   // orange-500
  [70, "#ef4444"],   // red-500
  [100, "#991b1b"],  // red-800 (most expensive)
];

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
 * Uses case expression on has_rate_data / has_reference_only properties.
 */
export function coverageFillExpression() {
  return [
    "case",
    ["==", ["get", "has_rate_data"], true],
    COVERAGE_COLORS.hasData,
    ["==", ["get", "has_reference_only"], true],
    COVERAGE_COLORS.referenceOnly,
    COVERAGE_COLORS.noData,
  ];
}

/**
 * Build a Maplibre fill-color expression for bill-at-10CCF mode.
 * Uses interpolate on bill_10ccf, falling back to gray for nulls.
 */
export function billFillExpression() {
  return [
    "case",
    ["==", ["get", "bill_10ccf"], null],
    COVERAGE_COLORS.noData,
    [
      "interpolate",
      ["linear"],
      ["get", "bill_10ccf"],
      ...BILL_COLOR_STOPS.flat(),
    ],
  ];
}
