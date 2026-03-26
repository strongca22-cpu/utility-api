/**
 * Dropdown toggle to switch between Coverage Status and Bill Amount views.
 */

export default function LayerToggle({ mode, onChange }) {
  return (
    <select
      value={mode}
      onChange={(e) => onChange(e.target.value)}
      className="rounded border border-slate-600 bg-slate-700 px-3 py-1.5 text-sm text-slate-100 focus:border-blue-500 focus:outline-none"
    >
      <option value="coverage">Coverage Status</option>
      <option value="bill">Bill at 10 CCF</option>
    </select>
  );
}
