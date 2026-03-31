/**
 * Hook to compute coverage stats dynamically from GeoJSON features
 * based on the current tier filter settings.
 *
 * Returns stats object shaped like coverage_stats.json but filtered.
 */

import { useMemo } from "react";

export function useDynamicStats(geojson, settings) {
  return useMemo(() => {
    if (!geojson?.features) return null;

    const showFree = settings?.showFree ?? true;
    const showPremium = settings?.showPremium ?? true;
    const showRef = settings?.showReference ?? true;

    let totalCws = 0;
    let withRateData = 0;
    let withReference = 0;
    let noData = 0;
    let popTotal = 0;
    let popCommercial = 0;  // free + premium
    let popReference = 0;
    let popFree = 0;
    let popPremium = 0;

    // QA counts
    let flaggedCount = 0;
    let staleCount = 0;
    let highVarianceCount = 0;

    for (const feat of geojson.features) {
      const p = feat.properties;
      const pop = p.population_served || 0;
      const tier = p.data_tier;

      totalCws++;
      popTotal += pop;

      if (tier === "free") {
        if (showFree) {
          withRateData++;
          popCommercial += pop;
        }
        popFree += pop;
      } else if (tier === "premium") {
        if (showPremium) {
          withRateData++;
          popCommercial += pop;
        }
        popPremium += pop;
      } else if (tier === "reference") {
        if (showRef) {
          withReference++;
          popReference += pop;
        }
      } else {
        noData++;
      }

      // QA stats (counted regardless of tier filter)
      if (p.needs_review) flaggedCount++;
      if (p.is_stale && p.has_rate_data) staleCount++;
      if (p.has_high_variance && p.has_rate_data) highVarianceCount++;
    }

    const visibleCovered = withRateData + withReference;
    const visiblePop = popCommercial + popReference;

    return {
      total_cws: totalCws,
      with_rate_data: withRateData,
      with_reference: withReference,
      no_data: noData,
      pct_covered: totalCws > 0 ? (visibleCovered / totalCws) * 100 : 0,
      population_total: popTotal,
      population_covered: visiblePop,
      population_commercial: popCommercial,
      population_reference: popReference,
      population_free: popFree,
      population_premium: popPremium,
      pct_population: popTotal > 0 ? (visiblePop / popTotal) * 100 : 0,
      // QA
      flagged_count: flaggedCount,
      stale_count: staleCount,
      high_variance_count: highVarianceCount,
    };
  }, [geojson, settings?.showFree, settings?.showPremium, settings?.showReference]);
}
