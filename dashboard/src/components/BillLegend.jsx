/**
 * Legend overlay for the Bill at 10 CCF view.
 * Shows the active color ramp with labeled stops.
 * Positioned bottom-left over the map.
 */

import { BILL_RAMPS, DEFAULT_BILL_RAMP, TIER_COLORS } from "../utils/colors";

export default function BillLegend({ rampKey, visible }) {
  if (!visible) return null;

  const ramp = BILL_RAMPS[rampKey] || BILL_RAMPS[DEFAULT_BILL_RAMP];
  const stops = ramp.stops;

  // Build gradient CSS
  const gradientStops = stops
    .map((s) => {
      const pct = (s[0] / stops[stops.length - 1][0]) * 100;
      return `${s[1]} ${pct}%`;
    })
    .join(", ");

  return (
    <div className="absolute bottom-4 left-12 z-40 rounded-lg bg-slate-900/90 border border-slate-700 px-3 py-2.5 shadow-lg">
      <div className="text-xs font-medium text-slate-400 mb-1.5">
        Monthly Bill @ 10 CCF
      </div>

      {/* Gradient bar */}
      <div
        className="h-3 w-48 rounded-sm"
        style={{ background: `linear-gradient(to right, ${gradientStops})` }}
      />

      {/* Labels */}
      <div className="flex justify-between mt-1 text-[10px] text-slate-500 w-48">
        <span>${stops[0][0]}</span>
        <span>${stops[Math.floor(stops.length / 2)][0]}</span>
        <span>${stops[stops.length - 1][0]}+</span>
      </div>

      {/* No data indicator */}
      <div className="flex items-center gap-1.5 mt-1.5 text-[10px] text-slate-500">
        <span
          className="inline-block w-3 h-3 rounded-sm"
          style={{ backgroundColor: TIER_COLORS.noData, opacity: 0.3 }}
        />
        No data
      </div>
    </div>
  );
}
