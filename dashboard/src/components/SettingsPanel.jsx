/**
 * Settings dropdown panel — triggered by gear icon in top bar.
 * Controls: data tier filter, fill opacity slider, visibility toggles.
 */

import { useRef, useEffect } from "react";
import { TIER_COLORS } from "../utils/colors";

export default function SettingsPanel({ settings, onChange, onClose }) {
  const panelRef = useRef(null);

  // Close on click outside
  useEffect(() => {
    function handleClick(e) {
      if (panelRef.current && !panelRef.current.contains(e.target)) {
        onClose();
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [onClose]);

  // Close on ESC
  useEffect(() => {
    function handleKey(e) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [onClose]);

  return (
    <div
      ref={panelRef}
      className="absolute right-0 top-full mt-1 w-72 rounded-lg border border-slate-600 bg-slate-800 p-4 shadow-xl z-50"
    >
      {/* Data tier filter */}
      <div className="mb-4">
        <div className="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">
          Data Tier
        </div>
        <div className="space-y-1.5">
          <TierCheckbox
            label="Free / Government"
            sublabel="EFC, eAR, PUC filings"
            color={TIER_COLORS.free}
            checked={settings.showFree}
            onChange={(v) => onChange({ ...settings, showFree: v })}
          />
          <TierCheckbox
            label="Premium"
            sublabel="LLM-scraped rates"
            color={TIER_COLORS.premium}
            checked={settings.showPremium}
            onChange={(v) => onChange({ ...settings, showPremium: v })}
          />
          <TierCheckbox
            label="Reference"
            sublabel="Duke NIEPS (internal only)"
            color={TIER_COLORS.reference}
            checked={settings.showReference}
            onChange={(v) => onChange({ ...settings, showReference: v })}
          />
        </div>
      </div>

      <div className="border-t border-slate-700 pt-3">
        <div className="text-xs font-medium text-slate-400 uppercase tracking-wider mb-2">
          Display
        </div>

        {/* Fill opacity */}
        <label className="block mb-3">
          <div className="flex justify-between text-sm text-slate-300 mb-1">
            <span>Fill Opacity</span>
            <span className="text-slate-500">{Math.round(settings.fillOpacity * 100)}%</span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            value={Math.round(settings.fillOpacity * 100)}
            onChange={(e) => onChange({ ...settings, fillOpacity: e.target.value / 100 })}
            className="w-full accent-blue-500"
          />
        </label>

        {/* Show no-data polygons */}
        <label className="flex items-center gap-2 mb-2 text-sm text-slate-300 cursor-pointer">
          <input
            type="checkbox"
            checked={settings.showNoData}
            onChange={(e) => onChange({ ...settings, showNoData: e.target.checked })}
            className="rounded border-slate-600 accent-slate-500"
          />
          Show no-data polygons
        </label>

        {/* Show outlines */}
        <label className="flex items-center gap-2 text-sm text-slate-300 cursor-pointer">
          <input
            type="checkbox"
            checked={settings.showOutlines}
            onChange={(e) => onChange({ ...settings, showOutlines: e.target.checked })}
            className="rounded border-slate-600 accent-blue-500"
          />
          Show polygon outlines
        </label>
      </div>
    </div>
  );
}

function TierCheckbox({ label, sublabel, color, checked, onChange }) {
  return (
    <label className="flex items-start gap-2 text-sm text-slate-300 cursor-pointer group">
      <input
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className="mt-0.5 rounded border-slate-600"
      />
      <div className="flex-1 flex items-center gap-2">
        <span className="inline-block w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ backgroundColor: color }} />
        <div>
          <div className="group-hover:text-slate-100 transition-colors">{label}</div>
          <div className="text-xs text-slate-500">{sublabel}</div>
        </div>
      </div>
    </label>
  );
}
