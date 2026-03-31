/**
 * Right-side detail panel — appears when a polygon is clicked.
 * Product mode: utility metadata, rate data, tier breakdown, source info.
 * QA mode: all of the above + source URL, variance, county comparison, flags.
 */

import { useEffect, useRef, useState } from "react";
import { formatCurrency, formatPopulation, ownerTypeLabel } from "../utils/format";
import { useDashboard } from "../contexts/DashboardContext";
import { TIER_COLORS } from "../utils/colors";
import { useCountyRates } from "../hooks/useCountyRates";

export default function DetailPanel({ utility, onClose }) {
  const panelRef = useRef(null);
  const { appMode } = useDashboard();

  // ESC to close
  useEffect(() => {
    function handleKey(e) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [onClose]);

  // Scroll to top when utility changes
  useEffect(() => {
    if (panelRef.current) panelRef.current.scrollTop = 0;
  }, [utility?.pwsid]);

  if (!utility) return null;

  // Parse tier detail from JSON string properties
  let tiers = null;
  let fixedCharges = null;
  try {
    if (utility.volumetric_tiers_json) {
      tiers = JSON.parse(utility.volumetric_tiers_json);
    }
    if (utility.fixed_charges_json) {
      fixedCharges = JSON.parse(utility.fixed_charges_json);
    }
  } catch {
    // Parsing failed
  }

  // Tier badge
  const tierBadge = utility.data_tier ? TIER_BADGE_MAP[utility.data_tier] : null;

  return (
    <div
      ref={panelRef}
      className="detail-panel flex flex-col overflow-y-auto border-l border-slate-700 bg-slate-900 animate-slide-in"
    >
      {/* Sticky header */}
      <div className="sticky top-0 z-10 bg-slate-900 border-b border-slate-700/50 px-4 py-3">
        <button
          onClick={onClose}
          className="float-right w-7 h-7 flex items-center justify-center rounded hover:bg-slate-700 text-slate-500 hover:text-slate-300 transition-colors"
          aria-label="Close panel"
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <path d="M1 1l12 12M13 1L1 13" />
          </svg>
        </button>
        <h2 className="text-base font-semibold text-slate-100 pr-8 leading-tight">
          {utility.pws_name || "Unknown Utility"}
        </h2>
        <div className="mt-0.5 text-xs text-slate-400 flex items-center gap-2">
          <span>{utility.pwsid}</span>
          {utility.owner_type && (
            <span> · {ownerTypeLabel(utility.owner_type)}</span>
          )}
          {tierBadge && (
            <span
              className="px-1.5 py-0.5 rounded text-[10px] font-semibold uppercase"
              style={{
                backgroundColor: tierBadge.color + "20",
                color: tierBadge.color,
              }}
            >
              {tierBadge.label}
            </span>
          )}
        </div>
      </div>

      {/* Scrollable body */}
      <div className="px-4 py-3 flex-1">
        <div className="text-sm text-slate-400">
          {[utility.county, utility.state].filter(Boolean).join(", ")}
        </div>
        {utility.population_served && (
          <div className="text-sm text-slate-400">
            Pop: {formatPopulation(utility.population_served)}
          </div>
        )}

        {/* Rate data section */}
        {utility.has_rate_data ? (
          <>
            <SectionDivider label="Rate Data" />

            <div className="space-y-1.5">
              <BillRow label="5 CCF" value={utility.bill_5ccf} />
              <BillRow label="10 CCF" value={utility.bill_10ccf} hero />
              <BillRow label="20 CCF" value={utility.bill_20ccf} />
            </div>

            {utility.rate_structure_type && (
              <div className="mt-3 text-sm text-slate-400">
                <span className="text-slate-500">Structure: </span>
                <span className="text-slate-200 capitalize">
                  {utility.rate_structure_type.replace(/_/g, " ")}
                </span>
              </div>
            )}

            {utility.fixed_charge != null && (
              <div className="text-sm text-slate-400">
                <span className="text-slate-500">Fixed charge: </span>
                <span className="text-slate-200">
                  {formatCurrency(utility.fixed_charge)}/mo
                </span>
              </div>
            )}

            {/* Tier breakdown */}
            {tiers && tiers.length > 0 && (
              <div className="mt-3">
                <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                  Volumetric Tiers
                </div>
                <div className="space-y-0.5 text-sm">
                  {tiers.map((tier, i) => (
                    <div key={i} className="flex justify-between text-slate-300">
                      <span>
                        {formatGal(tier.min_gal)}–{tier.max_gal ? formatGal(tier.max_gal) : "\u221e"}
                      </span>
                      <span className="text-slate-200">
                        ${Number(tier.rate_per_1000_gal).toFixed(2)}/kgal
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Fixed charge breakdown */}
            {fixedCharges && fixedCharges.length > 0 && (
              <div className="mt-3">
                <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                  Fixed Charges
                </div>
                <div className="space-y-0.5 text-sm">
                  {fixedCharges.map((fc, i) => (
                    <div key={i} className="flex justify-between text-slate-300">
                      <span>{fc.name || "Service charge"}</span>
                      <span className="text-slate-200">
                        {formatCurrency(fc.amount)}/mo
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Source info */}
            <SectionDivider label="Source" />
            <div className="space-y-0.5 text-sm text-slate-400">
              {utility.source_name && (
                <div className="text-slate-200">{utility.source_name}</div>
              )}
              {utility.data_vintage && (
                <div>
                  <span className="text-slate-500">Vintage: </span>
                  {utility.data_vintage}
                  {utility.is_stale && (
                    <span className="ml-1 text-orange-400 text-xs">(stale)</span>
                  )}
                </div>
              )}
              {utility.confidence && (
                <div>
                  <span className="text-slate-500">Confidence: </span>
                  <span className="capitalize">{utility.confidence}</span>
                </div>
              )}
            </div>

            {/* QA Mode: extended details */}
            {appMode === "qa" && (
              <QASection utility={utility} />
            )}
          </>
        ) : (
          <>
            <SectionDivider label="No Rate Data" />
            <p className="text-sm text-slate-500">
              No commercial rate data available for this utility.
            </p>
            {utility.has_reference_only && (
              <p className="mt-2 text-sm text-amber-500/80">
                Reference data available (Duke NIEPS, internal only)
              </p>
            )}
          </>
        )}
      </div>
    </div>
  );
}

/* --- QA Section (only in QA mode) --- */

function QASection({ utility }) {
  const countyRates = useCountyRates(utility.county, utility.state);
  const [flagged, setFlagged] = useState(() => {
    const flags = JSON.parse(localStorage.getItem("uapi_flags") || "{}");
    return !!flags[utility.pwsid];
  });

  function toggleFlag() {
    const flags = JSON.parse(localStorage.getItem("uapi_flags") || "{}");
    if (flagged) {
      delete flags[utility.pwsid];
    } else {
      flags[utility.pwsid] = new Date().toISOString();
    }
    localStorage.setItem("uapi_flags", JSON.stringify(flags));
    setFlagged(!flagged);
  }

  // Reset flag state when utility changes
  useEffect(() => {
    const flags = JSON.parse(localStorage.getItem("uapi_flags") || "{}");
    setFlagged(!!flags[utility.pwsid]);
  }, [utility.pwsid]);

  return (
    <>
      <SectionDivider label="QA Details" />

      {/* Source URL */}
      {utility.source_url && (
        <div className="text-sm mb-2">
          <span className="text-slate-500">Source: </span>
          <a
            href={utility.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-400 hover:text-blue-300 underline break-all"
          >
            {utility.source_url.length > 60
              ? utility.source_url.slice(0, 60) + "..."
              : utility.source_url}
          </a>
        </div>
      )}

      {/* Sources + selection */}
      <div className="space-y-0.5 text-sm text-slate-400">
        {utility.n_sources && (
          <div>
            <span className="text-slate-500">Sources: </span>
            {utility.n_sources} distinct
          </div>
        )}
        {utility.parse_model && (
          <div>
            <span className="text-slate-500">Parse model: </span>
            {utility.parse_model}
          </div>
        )}
        {utility.last_scraped && (
          <div>
            <span className="text-slate-500">Last scraped: </span>
            {utility.last_scraped.split("T")[0]}
          </div>
        )}
        {utility.conservation_signal != null && (
          <div>
            <span className="text-slate-500">Conservation signal: </span>
            {utility.conservation_signal.toFixed(2)}x
          </div>
        )}
        {utility.selection_notes && (
          <div>
            <span className="text-slate-500">Selection: </span>
            {utility.selection_notes}
          </div>
        )}
      </div>

      {/* Variance */}
      {utility.bill_range_min != null && utility.bill_range_max != null && (
        <div className="mt-2 text-sm">
          <span className="text-slate-500">Bill range: </span>
          <span className={utility.has_high_variance ? "text-yellow-400" : "text-slate-300"}>
            {formatCurrency(utility.bill_range_min)} – {formatCurrency(utility.bill_range_max)}
          </span>
          {utility.has_high_variance && (
            <span className="ml-1 text-yellow-400 text-xs">(high variance)</span>
          )}
        </div>
      )}

      {/* Review flags */}
      {utility.needs_review && (
        <div className="mt-2 px-2 py-1.5 rounded bg-red-900/20 border border-red-800/30 text-sm">
          <span className="text-red-400 font-medium">Flagged for review</span>
          {utility.review_reason && (
            <div className="text-red-400/70 text-xs mt-0.5">{utility.review_reason}</div>
          )}
        </div>
      )}

      {/* County comparison */}
      {countyRates && countyRates.length > 0 && (
        <div className="mt-3">
          <div className="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
            County Comparison ({utility.county})
          </div>
          <div className="space-y-0.5 text-sm max-h-40 overflow-y-auto">
            {countyRates.map((r) => (
              <div
                key={r.pwsid}
                className={`flex justify-between text-slate-400 ${
                  r.pwsid === utility.pwsid ? "text-blue-300 font-medium" : ""
                }`}
              >
                <span className="truncate mr-2" title={r.name}>
                  {r.name?.slice(0, 25) || r.pwsid}
                </span>
                <span className="flex-shrink-0">{formatCurrency(r.bill_10ccf)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Flag button */}
      <div className="mt-4">
        <button
          onClick={toggleFlag}
          className={`w-full text-sm py-2 rounded transition-colors ${
            flagged
              ? "bg-red-600/20 text-red-400 border border-red-600/40 hover:bg-red-600/30"
              : "bg-slate-700 text-slate-300 hover:bg-slate-600"
          }`}
        >
          {flagged ? "Remove Flag" : "Flag for Review"}
        </button>
      </div>
    </>
  );
}

/* --- Sub-components --- */

const TIER_BADGE_MAP = {
  free: { label: "Free", color: TIER_COLORS.free },
  premium: { label: "Premium", color: TIER_COLORS.premium },
  reference: { label: "Reference", color: TIER_COLORS.reference },
};

function SectionDivider({ label }) {
  return (
    <div className="mt-4 mb-2 flex items-center gap-2">
      <span className="text-xs font-medium text-slate-500 uppercase tracking-wider">
        {label}
      </span>
      <div className="flex-1 border-t border-slate-700" />
    </div>
  );
}

function BillRow({ label, value, hero = false }) {
  return (
    <div className="flex justify-between items-baseline">
      <span className="text-sm text-slate-500">{label}:</span>
      <span
        className={
          hero
            ? "text-xl font-semibold text-blue-400"
            : "text-sm text-slate-200"
        }
      >
        {formatCurrency(value)}
      </span>
    </div>
  );
}

function formatGal(gal) {
  if (gal == null) return "0";
  if (gal >= 1000) return `${(gal / 1000).toFixed(0)}K`;
  return String(gal);
}
