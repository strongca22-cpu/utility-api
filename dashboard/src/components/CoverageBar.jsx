/**
 * Bottom bar showing coverage summary with split population display.
 * Dynamically reflects current tier filter settings.
 *
 * Product mode: green (Premium = free+premium), amber (Free = reference), gray (no data).
 * QA mode:      green (free), blue (premium), amber (reference), gray (no data).
 */

import { formatCompact, formatPct } from "../utils/format";
import { TIER_COLORS } from "../utils/colors";
import { useDashboard } from "../contexts/DashboardContext";

export default function CoverageBar({ stats, staticStats }) {
  const { appMode } = useDashboard();

  if (!stats) return null;

  const totalVisible = stats.with_rate_data + stats.with_reference;
  const pt = stats.population_total || 1; // avoid division by zero

  // Format the generated_at timestamp from static stats
  let refreshLabel = null;
  if (staticStats?.generated_at) {
    try {
      const d = new Date(staticStats.generated_at);
      refreshLabel = d.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } catch {
      // ignore
    }
  }

  // Product mode: merge free+premium into one "Premium" segment
  if (appMode === "product") {
    const premiumPop = (stats.population_free || 0) + (stats.population_premium || 0);
    const premiumPct = (premiumPop / pt) * 100;
    const freePct = ((stats.population_reference || 0) / pt) * 100;

    return (
      <div className="relative z-10 flex items-center gap-4 bg-slate-800 px-4 py-2 text-sm text-slate-300 border-t border-slate-700">
        <span>
          <span className="text-slate-100 font-medium">
            {totalVisible.toLocaleString()}
          </span>{" "}
          / {stats.total_cws.toLocaleString()} PWSIDs ({formatPct(stats.pct_covered)})
        </span>

        {/* Progress bar — green (Premium) | amber (Free) */}
        <div className="flex-1 max-w-sm h-2.5 bg-slate-700 rounded-full overflow-hidden flex">
          {premiumPct > 0 && (
            <div
              className="h-full transition-all"
              style={{ width: `${premiumPct}%`, backgroundColor: TIER_COLORS.free }}
              title={`Premium: ${formatCompact(premiumPop)}`}
            />
          )}
          {freePct > 0 && (
            <div
              className="h-full transition-all"
              style={{ width: `${freePct}%`, backgroundColor: TIER_COLORS.reference }}
              title={`Free: ${formatCompact(stats.population_reference)}`}
            />
          )}
        </div>

        <span className="flex items-center gap-1.5">
          <span className="text-slate-100 font-medium">
            {formatCompact(stats.population_covered)}
          </span>
          <span className="text-slate-500">/</span>
          <span>{formatCompact(stats.population_total)}</span>
          <span>pop</span>
          <span>({formatPct(stats.pct_population)})</span>
        </span>

        {/* Tier chips: Premium (green) | Free (amber) */}
        <span className="text-xs text-slate-500 border-l border-slate-700 pl-3 ml-1 flex gap-2">
          {premiumPop > 0 && (
            <span className="flex items-center gap-1">
              <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: TIER_COLORS.free }} />
              {formatCompact(premiumPop)} Premium
            </span>
          )}
          {stats.population_reference > 0 && (
            <span className="flex items-center gap-1">
              <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: TIER_COLORS.reference }} />
              {formatCompact(stats.population_reference)} Free
            </span>
          )}
        </span>

        {refreshLabel && (
          <span className="text-slate-500 text-xs ml-auto">
            {refreshLabel}
          </span>
        )}
      </div>
    );
  }

  // QA mode: full source breakdown
  const freePct = (stats.population_free / pt) * 100;
  const premiumPct = (stats.population_premium / pt) * 100;
  const referencePct = (stats.population_reference / pt) * 100;

  return (
    <div className="relative z-10 flex items-center gap-4 bg-slate-800 px-4 py-2 text-sm text-slate-300 border-t border-slate-700">
      {/* PWSID count */}
      <span>
        <span className="text-slate-100 font-medium">
          {totalVisible.toLocaleString()}
        </span>{" "}
        / {stats.total_cws.toLocaleString()} PWSIDs ({formatPct(stats.pct_covered)})
      </span>

      {/* Split progress bar — green | blue | amber */}
      <div className="flex-1 max-w-sm h-2.5 bg-slate-700 rounded-full overflow-hidden flex">
        {freePct > 0 && (
          <div
            className="h-full transition-all"
            style={{ width: `${freePct}%`, backgroundColor: TIER_COLORS.free }}
            title={`Free/Gov: ${formatCompact(stats.population_free)}`}
          />
        )}
        {premiumPct > 0 && (
          <div
            className="h-full transition-all"
            style={{ width: `${premiumPct}%`, backgroundColor: TIER_COLORS.premium }}
            title={`Premium: ${formatCompact(stats.population_premium)}`}
          />
        )}
        {referencePct > 0 && (
          <div
            className="h-full transition-all"
            style={{ width: `${referencePct}%`, backgroundColor: TIER_COLORS.reference }}
            title={`Reference: ${formatCompact(stats.population_reference)}`}
          />
        )}
      </div>

      {/* Population summary */}
      <span className="flex items-center gap-1.5">
        <span className="text-slate-100 font-medium">
          {formatCompact(stats.population_covered)}
        </span>
        <span className="text-slate-500">/</span>
        <span>{formatCompact(stats.population_total)}</span>
        <span>pop</span>
        <span>({formatPct(stats.pct_population)})</span>
      </span>

      {/* Tier breakdown chips */}
      <span className="text-xs text-slate-500 border-l border-slate-700 pl-3 ml-1 flex gap-2">
        {stats.population_free > 0 && (
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: TIER_COLORS.free }} />
            {formatCompact(stats.population_free)}
          </span>
        )}
        {stats.population_premium > 0 && (
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: TIER_COLORS.premium }} />
            {formatCompact(stats.population_premium)}
          </span>
        )}
        {stats.population_reference > 0 && (
          <span className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: TIER_COLORS.reference }} />
            {formatCompact(stats.population_reference)}
          </span>
        )}
      </span>

      {refreshLabel && (
        <span className="text-slate-500 text-xs ml-auto">
          {refreshLabel}
        </span>
      )}
    </div>
  );
}
