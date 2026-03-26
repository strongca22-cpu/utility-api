/**
 * Bottom bar showing coverage summary: PWSIDs covered and population coverage.
 */

import { formatCompact, formatPct } from "../utils/format";

export default function CoverageBar({ stats }) {
  if (!stats) return null;

  const pctWidth = Math.max(1, stats.pct_covered);

  return (
    <div className="relative z-10 flex items-center gap-4 bg-slate-800 px-4 py-2 text-sm text-slate-300 border-t border-slate-700">
      <span>
        <span className="text-slate-100 font-medium">
          {stats.with_rate_data.toLocaleString()}
        </span>{" "}
        / {stats.total_cws.toLocaleString()} PWSIDs ({formatPct(stats.pct_covered)})
      </span>

      {/* Mini progress bar */}
      <div className="flex-1 max-w-xs h-2 bg-slate-700 rounded-full overflow-hidden">
        <div
          className="h-full bg-blue-500 rounded-full transition-all"
          style={{ width: `${pctWidth}%` }}
        />
      </div>

      <span>
        <span className="text-slate-100 font-medium">
          {formatCompact(stats.population_covered)}
        </span>{" "}
        / {formatCompact(stats.population_total)} pop ({formatPct(stats.pct_population)})
      </span>
    </div>
  );
}
