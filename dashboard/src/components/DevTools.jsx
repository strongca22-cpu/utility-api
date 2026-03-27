/**
 * Dev Tools sidebar — temporary panel for comparing visualization options.
 * Not for production. Toggle with Ctrl+Shift+D or the wrench icon.
 */

import { BILL_RAMPS, DEFAULT_BILL_RAMP } from "../utils/colors";

export default function DevTools({ billRamp, onBillRampChange, onClose }) {
  return (
    <div className="absolute top-0 left-0 bottom-0 z-50 w-64 bg-slate-950/95 border-r border-slate-700 p-4 overflow-y-auto shadow-2xl">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono bg-amber-600/20 text-amber-400 px-1.5 py-0.5 rounded">
            DEV
          </span>
          <span className="text-sm font-medium text-slate-300">Dev Tools</span>
        </div>
        <button
          onClick={onClose}
          className="w-6 h-6 flex items-center justify-center rounded hover:bg-slate-800 text-slate-500 hover:text-slate-300"
          aria-label="Close dev tools"
        >
          <svg width="12" height="12" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M1 1l12 12M13 1L1 13" />
          </svg>
        </button>
      </div>

      {/* Bill Color Ramp selector */}
      <div className="mb-4">
        <div className="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">
          Bill Color Ramp
        </div>
        <div className="space-y-2">
          {Object.entries(BILL_RAMPS).map(([key, ramp]) => (
            <RampOption
              key={key}
              rampKey={key}
              ramp={ramp}
              active={billRamp === key}
              onSelect={() => onBillRampChange(key)}
            />
          ))}
        </div>
      </div>

      {/* Info */}
      <div className="mt-6 text-[10px] text-slate-600 border-t border-slate-800 pt-3">
        <p>Toggle: <kbd className="bg-slate-800 px-1 rounded text-slate-500">Ctrl+Shift+D</kbd></p>
        <p className="mt-1">These ramps avoid green→red to prevent implying that low bills are "good" and high bills are "bad."</p>
      </div>
    </div>
  );
}

function RampOption({ rampKey, ramp, active, onSelect }) {
  const stops = ramp.stops;
  const gradientStops = stops
    .map((s) => {
      const pct = (s[0] / stops[stops.length - 1][0]) * 100;
      return `${s[1]} ${pct}%`;
    })
    .join(", ");

  return (
    <button
      onClick={onSelect}
      className={`w-full text-left rounded-md p-2 transition-colors ${
        active
          ? "bg-slate-800 ring-1 ring-blue-500/50"
          : "hover:bg-slate-800/50"
      }`}
    >
      <div className="flex items-center justify-between mb-1">
        <span className={`text-sm ${active ? "text-slate-100 font-medium" : "text-slate-400"}`}>
          {ramp.name}
        </span>
        {active && (
          <span className="text-[10px] text-blue-400 font-medium">ACTIVE</span>
        )}
      </div>
      {/* Gradient preview */}
      <div
        className="h-4 w-full rounded-sm"
        style={{ background: `linear-gradient(to right, ${gradientStops})` }}
      />
      <div className="flex justify-between mt-0.5 text-[9px] text-slate-600">
        <span>${stops[0][0]}/mo</span>
        <span>${stops[stops.length - 1][0]}+/mo</span>
      </div>
      <div className="text-[10px] text-slate-600 mt-0.5">{ramp.description}</div>
    </button>
  );
}
