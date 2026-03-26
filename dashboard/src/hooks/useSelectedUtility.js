/**
 * Hook to manage the currently selected utility (clicked polygon).
 */

import { useState, useCallback } from "react";

export function useSelectedUtility() {
  const [selected, setSelected] = useState(null);

  const select = useCallback((properties) => {
    setSelected(properties);
  }, []);

  const deselect = useCallback(() => {
    setSelected(null);
  }, []);

  return { selected, select, deselect };
}
