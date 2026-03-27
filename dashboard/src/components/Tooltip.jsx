/**
 * Map hover tooltip — shows utility name, owner type, and bill amount.
 * Smart positioning: flips away from viewport edges.
 */

import { formatCurrency, ownerTypeLabel } from "../utils/format";

const TOOLTIP_W = 260;
const TOOLTIP_H = 80;
const OFFSET = 12;
const EDGE_PAD = 8;

export default function Tooltip({ feature, x, y, containerWidth, containerHeight }) {
  if (!feature) return null;

  const props = feature.properties;
  const bill = props.bill_10ccf;

  // Smart position: flip if near edges
  const cw = containerWidth || window.innerWidth;
  const ch = containerHeight || window.innerHeight;

  let left = x + OFFSET;
  let top = y + OFFSET;

  if (left + TOOLTIP_W + EDGE_PAD > cw) {
    left = x - TOOLTIP_W - OFFSET;
  }
  if (top + TOOLTIP_H + EDGE_PAD > ch) {
    top = y - TOOLTIP_H - OFFSET;
  }
  // Clamp to viewport
  left = Math.max(EDGE_PAD, left);
  top = Math.max(EDGE_PAD, top);

  return (
    <div
      className="pointer-events-none absolute z-50 rounded bg-slate-900/95 px-3 py-2 text-sm text-slate-100 shadow-lg"
      style={{ left, top, maxWidth: TOOLTIP_W }}
    >
      <div className="font-semibold">{props.pws_name || "Unknown"}</div>
      <div className="text-slate-400 text-xs">
        {props.pwsid}
        {props.owner_type && (
          <span> · {ownerTypeLabel(props.owner_type)}</span>
        )}
      </div>
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
