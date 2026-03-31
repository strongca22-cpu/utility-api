import { createContext, useContext, useReducer } from "react";

const DashboardContext = createContext(null);
const DashboardDispatch = createContext(null);

const initialState = {
  // Mode
  appMode: "product", // "product" | "qa"

  // View
  layerMode: "coverage", // "coverage" | "bill"
  billRamp: "violet",

  // Data layers (tier filters)
  showPremium: true,
  showFree: true,
  showReference: true,
  showNoData: false,

  // Display
  fillOpacity: 0.75,
  showOutlines: false,
  showStateBoundaries: false,
  showCountyBoundaries: false,

  // QA filters
  qaShowFlagged: false,
  qaShowHighVariance: false,
  qaShowStale: false,

  // Selection
  selected: null,

  // UI chrome
  devToolsOpen: false,
};

function dashboardReducer(state, action) {
  switch (action.type) {
    case "SET_MODE":
      return { ...state, appMode: action.payload };
    case "SET_LAYER_MODE":
      return { ...state, layerMode: action.payload };
    case "SET_BILL_RAMP":
      return { ...state, billRamp: action.payload };
    case "TOGGLE_TIER": {
      const key = action.payload; // "showPremium" | "showFree" | "showReference" | "showNoData"
      return { ...state, [key]: !state[key] };
    }
    case "SET_OPACITY":
      return { ...state, fillOpacity: action.payload };
    case "TOGGLE_OUTLINES":
      return { ...state, showOutlines: !state.showOutlines };
    case "TOGGLE_STATE_BOUNDARIES":
      return { ...state, showStateBoundaries: !state.showStateBoundaries };
    case "TOGGLE_COUNTY_BOUNDARIES":
      return { ...state, showCountyBoundaries: !state.showCountyBoundaries };
    case "TOGGLE_QA_FLAGGED":
      return { ...state, qaShowFlagged: !state.qaShowFlagged };
    case "TOGGLE_QA_HIGH_VARIANCE":
      return { ...state, qaShowHighVariance: !state.qaShowHighVariance };
    case "TOGGLE_QA_STALE":
      return { ...state, qaShowStale: !state.qaShowStale };
    case "SELECT_UTILITY":
      return { ...state, selected: action.payload };
    case "DESELECT":
      return { ...state, selected: null };
    case "TOGGLE_DEVTOOLS":
      return { ...state, devToolsOpen: !state.devToolsOpen };
    default:
      return state;
  }
}

export function DashboardProvider({ children }) {
  const [state, dispatch] = useReducer(dashboardReducer, initialState);
  return (
    <DashboardContext.Provider value={state}>
      <DashboardDispatch.Provider value={dispatch}>
        {children}
      </DashboardDispatch.Provider>
    </DashboardContext.Provider>
  );
}

export function useDashboard() {
  const ctx = useContext(DashboardContext);
  if (!ctx) throw new Error("useDashboard must be used within DashboardProvider");
  return ctx;
}

export function useDashboardDispatch() {
  const ctx = useContext(DashboardDispatch);
  if (!ctx) throw new Error("useDashboardDispatch must be used within DashboardProvider");
  return ctx;
}
