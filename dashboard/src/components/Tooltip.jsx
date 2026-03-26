/**
 * Map hover tooltip — shows utility name and bill amount.
 * Positioned absolutely near the cursor via x/y props.
 */

import { formatCurrency } from "../utils/format";

export default function Tooltip({ feature, x, y }) {
  if (!feature) return null;

  const props = feature.properties;
  const bill = props.bill_10ccf;

  return (
    <div
      className="pointer-events-none absolute z-50 rounded bg-slate-900/95 px-3 py-2 text-sm text-slate-100 shadow-lg"
      style={{
        left: x + 12,
        top: y + 12,
        maxWidth: 280,
      }}
    >
      <div className="font-semibold">{props.pws_name || "Unknown"}</div>
      <div className="text-slate-400 text-xs">{props.pwsid}</div>
      {props.has_rate_data ? (
        <div className="mt-1 text-blue-400">
          Bill @10CCF: {formatCurrency(bill)}
        </div>
      ) : props.has_reference_only ? (
        <div className="mt-1 text-amber-400">Reference data only</div>
      ) : (
        <div className="mt-1 text-slate-500">No rate data</div>
      )}
    </div>
  );
}
