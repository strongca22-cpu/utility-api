/**
 * Formatting helpers for currency, population, and other display values.
 */

/**
 * Format a number as US currency (e.g., $48.15).
 * Returns "—" for null/undefined.
 */
export function formatCurrency(value) {
  if (value == null) return "—";
  return `$${Number(value).toFixed(2)}`;
}

/**
 * Format a population number with commas (e.g., 467,665).
 * Returns "—" for null/undefined.
 */
export function formatPopulation(value) {
  if (value == null) return "—";
  return Number(value).toLocaleString("en-US");
}

/**
 * Format a large number compactly (e.g., 189.7M, 2.8K).
 */
export function formatCompact(value) {
  if (value == null) return "—";
  const n = Number(value);
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString("en-US");
}

/**
 * Format a percentage (e.g., 20.7%).
 */
export function formatPct(value) {
  if (value == null) return "—";
  return `${Number(value).toFixed(1)}%`;
}

/**
 * Expand owner_type_code to readable label.
 */
export function ownerTypeLabel(code) {
  const labels = {
    F: "Federal",
    S: "State",
    L: "Local",
    M: "Municipal",
    P: "Private",
    N: "Native American",
  };
  return labels[code] || code || "Unknown";
}
