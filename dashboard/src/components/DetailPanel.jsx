/**
 * Right-side detail panel — appears when a polygon is clicked.
 * Shows utility metadata, rate data, tier breakdown, and source info.
 */

import { formatCurrency, formatPopulation, ownerTypeLabel } from "../utils/format";

export default function DetailPanel({ utility, onClose }) {
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
    // Parsing failed — show without tier detail
  }

  return (
    <div className="detail-panel overflow-y-auto border-l border-slate-700 bg-slate-900 p-4">
      {/* Close button */}
      <button
        onClick={onClose}
        className="float-right text-slate-500 hover:text-slate-300 text-lg leading-none"
        aria-label="Close"
      >
        ✕
      </button>

      {/* Header */}
      <h2 className="text-lg font-semibold text-slate-100 pr-6 leading-tight">
        {utility.pws_name || "Unknown Utility"}
      </h2>
      <div className="mt-1 text-sm text-slate-400">
        {utility.pwsid}
        {utility.owner_type && (
          <span> · {ownerTypeLabel(utility.owner_type)}</span>
        )}
      </div>
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
                      {formatGal(tier.min_gal)}–{tier.max_gal ? formatGal(tier.max_gal) : "∞"}
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
            {utility.source_tier && (
              <div>
                <span className="text-slate-500">Tier: </span>
                <span className="capitalize">{utility.source_tier.replace(/_/g, " ")}</span>
              </div>
            )}
            {utility.data_vintage && (
              <div>
                <span className="text-slate-500">Vintage: </span>
                {utility.data_vintage}
              </div>
            )}
            {utility.confidence && (
              <div>
                <span className="text-slate-500">Confidence: </span>
                <span className="capitalize">{utility.confidence}</span>
              </div>
            )}
          </div>
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
  );
}

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
