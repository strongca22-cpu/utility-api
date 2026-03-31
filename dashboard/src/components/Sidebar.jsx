/**
 * Left sidebar — persistent controls for the dashboard.
 * Replaces the top bar + SettingsPanel dropdown with a fixed sidebar.
 *
 * Sections: Header, View, Data Layers, Display, QA (conditional).
 */

import { TIER_COLORS } from "../utils/colors";
import { useDashboard, useDashboardDispatch } from "../contexts/DashboardContext";
import { formatCompact, formatPct } from "../utils/format";

export default function Sidebar({ dynamicStats }) {
  const state = useDashboard();
  const dispatch = useDashboardDispatch();

  return (
    <div className="h-full flex flex-col bg-slate-900 border-r border-slate-700 overflow-y-auto sidebar-scroll">
      {/* Header */}
      <div className="p-4 pb-3 border-b border-slate-700">
        <div className="flex items-center justify-between mb-2">
          <h1 className="text-lg font-semibold text-slate-100">UAPI Dashboard</h1>
          <button
            onClick={() => dispatch({ type: "TOGGLE_SIDEBAR" })}
            className="w-6 h-6 flex items-center justify-center rounded text-slate-500 hover:text-slate-200 hover:bg-slate-700 transition-colors"
            aria-label="Collapse sidebar"
            title="Collapse sidebar"
          >
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
              <path d="M8 1L3 6l5 5" />
            </svg>
          </button>
        </div>
        <div className="flex gap-1 bg-slate-800 rounded-lg p-0.5">
          <ModeButton
            label="Product"
            active={state.appMode === "product"}
            onClick={() => dispatch({ type: "SET_MODE", payload: "product" })}
          />
          <ModeButton
            label="QA"
            active={state.appMode === "qa"}
            onClick={() => dispatch({ type: "SET_MODE", payload: "qa" })}
          />
        </div>
      </div>

      {/* VIEW section */}
      <Section label="View">
        <div className="space-y-1">
          <RadioOption
            label="Coverage"
            active={state.layerMode === "coverage"}
            onClick={() => dispatch({ type: "SET_LAYER_MODE", payload: "coverage" })}
          />
          <RadioOption
            label="Bill at 10 CCF"
            active={state.layerMode === "bill"}
            onClick={() => dispatch({ type: "SET_LAYER_MODE", payload: "bill" })}
          />
        </div>
      </Section>

      {/* DATA LAYERS section */}
      <Section label="Data Layers">
        <div className="space-y-1.5">
          <TierCheckbox
            label="Premium"
            sublabel="LLM-scraped rates"
            color={TIER_COLORS.premium}
            checked={state.showPremium}
            onChange={() => dispatch({ type: "TOGGLE_TIER", payload: "showPremium" })}
          />
          <TierCheckbox
            label="Free / Attributed"
            sublabel="EFC, eAR, PUC filings"
            color={TIER_COLORS.free}
            checked={state.showFree}
            onChange={() => dispatch({ type: "TOGGLE_TIER", payload: "showFree" })}
          />
          <TierCheckbox
            label="Reference"
            sublabel="Duke NIEPS"
            color={TIER_COLORS.reference}
            checked={state.showReference}
            onChange={() => dispatch({ type: "TOGGLE_TIER", payload: "showReference" })}
          />
          <TierCheckbox
            label="No Data"
            sublabel=""
            color={TIER_COLORS.noData}
            checked={state.showNoData}
            onChange={() => dispatch({ type: "TOGGLE_TIER", payload: "showNoData" })}
          />
        </div>

        {/* Live coverage stats */}
        {dynamicStats && (
          <div className="mt-3 pt-2 border-t border-slate-700/50 text-xs text-slate-400 space-y-0.5">
            <div>
              Coverage:{" "}
              <span className="text-slate-200">
                {(dynamicStats.with_rate_data || 0).toLocaleString()}
              </span>{" "}
              / {(dynamicStats.total_cws || 0).toLocaleString()}
            </div>
            <div>
              Pop:{" "}
              <span className="text-slate-200">
                {formatCompact(dynamicStats.population_covered || 0)}
              </span>{" "}
              ({formatPct(dynamicStats.pct_population || 0)})
            </div>
          </div>
        )}
      </Section>

      {/* DISPLAY section */}
      <Section label="Display">
        <label className="block mb-3">
          <div className="flex justify-between text-sm text-slate-300 mb-1">
            <span>Fill Opacity</span>
            <span className="text-slate-500">{Math.round(state.fillOpacity * 100)}%</span>
          </div>
          <input
            type="range"
            min="0"
            max="100"
            value={Math.round(state.fillOpacity * 100)}
            onChange={(e) =>
              dispatch({ type: "SET_OPACITY", payload: e.target.value / 100 })
            }
            className="w-full accent-blue-500"
          />
        </label>

        <div className="space-y-2">
          <Toggle
            label="Polygon boundaries"
            checked={state.showOutlines}
            onChange={() => dispatch({ type: "TOGGLE_OUTLINES" })}
          />
          <Toggle
            label="State boundaries"
            checked={state.showStateBoundaries}
            onChange={() => dispatch({ type: "TOGGLE_STATE_BOUNDARIES" })}
          />
          <Toggle
            label="County boundaries"
            checked={state.showCountyBoundaries}
            onChange={() => dispatch({ type: "TOGGLE_COUNTY_BOUNDARIES" })}
          />
        </div>
      </Section>

      {/* QA section (only in QA mode) */}
      {state.appMode === "qa" && (
        <Section label="QA">
          <div className="space-y-2">
            <Toggle
              label="Show flagged PWSIDs"
              checked={state.qaShowFlagged}
              onChange={() => dispatch({ type: "TOGGLE_QA_FLAGGED" })}
            />
            <Toggle
              label="Show high-variance"
              checked={state.qaShowHighVariance}
              onChange={() => dispatch({ type: "TOGGLE_QA_HIGH_VARIANCE" })}
            />
            <Toggle
              label="Show stale >2yr"
              checked={state.qaShowStale}
              onChange={() => dispatch({ type: "TOGGLE_QA_STALE" })}
            />
          </div>
          {dynamicStats && (
            <div className="mt-3 pt-2 border-t border-slate-700/50 text-xs text-slate-400 space-y-0.5">
              <div>
                Flagged: <span className="text-red-400">{dynamicStats.flagged_count || 0}</span>
              </div>
              <div>
                High-variance: <span className="text-yellow-400">{dynamicStats.high_variance_count || 0}</span>
              </div>
              <div>
                Stale: <span className="text-orange-400">{dynamicStats.stale_count || 0}</span>
              </div>
            </div>
          )}
        </Section>
      )}

      <div className="flex-1" /> {/* Spacer */}
    </div>
  );
}

/* --- Sub-components --- */

function Section({ label, children }) {
  return (
    <div className="px-4 py-3 border-b border-slate-700/50">
      <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-widest mb-2">
        {label}
      </div>
      {children}
    </div>
  );
}

function ModeButton({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`flex-1 text-xs font-medium py-1.5 rounded-md transition-colors ${
        active
          ? "bg-slate-700 text-slate-100 shadow-sm"
          : "text-slate-400 hover:text-slate-200"
      }`}
    >
      {label}
    </button>
  );
}

function RadioOption({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-2 text-sm px-2 py-1.5 rounded transition-colors ${
        active
          ? "bg-slate-700/60 text-slate-100"
          : "text-slate-400 hover:text-slate-200 hover:bg-slate-800"
      }`}
    >
      <span
        className={`w-3 h-3 rounded-full border-2 flex-shrink-0 ${
          active ? "border-blue-500 bg-blue-500" : "border-slate-500"
        }`}
      />
      {label}
    </button>
  );
}

function TierCheckbox({ label, sublabel, color, checked, onChange }) {
  return (
    <label className="flex items-start gap-2 text-sm text-slate-300 cursor-pointer group">
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        className="mt-0.5 rounded border-slate-600"
      />
      <div className="flex-1 flex items-center gap-2">
        <span
          className="inline-block w-2.5 h-2.5 rounded-full flex-shrink-0"
          style={{ backgroundColor: color }}
        />
        <div>
          <div className="group-hover:text-slate-100 transition-colors">{label}</div>
          {sublabel && <div className="text-xs text-slate-500">{sublabel}</div>}
        </div>
      </div>
    </label>
  );
}

function Toggle({ label, checked, onChange }) {
  return (
    <label className="flex items-center gap-2 text-sm text-slate-300 cursor-pointer">
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        className="rounded border-slate-600 accent-blue-500"
      />
      {label}
    </label>
  );
}
