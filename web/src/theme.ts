/**
 * Turbo Vision–inspired theme for YAGPCC.
 *
 * Two modes: **dark** (the original soft Turbo Vision) and **light**
 * (a warm light variant that keeps the retro monospace feel).
 */

export type ThemeMode = "dark" | "light";

/* ------------------------------------------------------------------ */
/*  Color palettes                                                     */
/* ------------------------------------------------------------------ */

export interface ThemeColors {
  /* Backgrounds */
  bgMain: string;
  bgSidebar: string;
  bgCard: string;
  bgCode: string;
  bgHover: string;
  bgSelected: string;
  bgSidebarHover: string;
  bgElevated: string;

  /* Text */
  textPrimary: string;
  textSecondary: string;
  textHeading: string;

  /* Accents */
  primary: string;
  cyan: string;
  green: string;
  yellow: string;
  red: string;
  orange: string;

  /* Borders */
  border: string;
  borderActive: string;

  /* Code block text */
  codeText: string;

  /* Modal overlay */
  modalOverlay: string;

  /* Alert warning */
  alertWarningBg: string;
  alertWarningBorder: string;
}

const DARK: ThemeColors = {
  bgMain: "#1e2a3a",
  bgSidebar: "#172231",
  bgCard: "#243447",
  bgCode: "#1a2535",
  bgHover: "#2d4156",
  bgSelected: "#2d4156",
  bgSidebarHover: "#253a4f",
  bgElevated: "#2a3b4f",

  textPrimary: "#c8d6e5",
  textSecondary: "#8899aa",
  textHeading: "#e0e8f0",

  primary: "#5ba4cf",
  cyan: "#6ec6c6",
  green: "#6abf8a",
  yellow: "#d4a95a",
  red: "#cf6b6b",
  orange: "#d4915a",

  border: "#2d4156",
  borderActive: "#3d5a75",

  codeText: "#6ec6c6",
  modalOverlay: "rgba(10, 16, 24, 0.7)",
  alertWarningBg: "#2a2520",
  alertWarningBorder: "#4a3a28",
};

const LIGHT: ThemeColors = {
  bgMain: "#f0ede8",
  bgSidebar: "#e4e0d8",
  bgCard: "#ffffff",
  bgCode: "#f5f2ec",
  bgHover: "#e8e4dc",
  bgSelected: "#ddd8ce",
  bgSidebarHover: "#d8d3c9",
  bgElevated: "#ffffff",

  textPrimary: "#2c3e50",
  textSecondary: "#6b7b8d",
  textHeading: "#1a2a3a",

  primary: "#3a7ca5",
  cyan: "#2a8a8a",
  green: "#3a8a5a",
  yellow: "#a07830",
  red: "#b04a4a",
  orange: "#b06a30",

  border: "#d0cbc2",
  borderActive: "#b8b0a4",

  codeText: "#2a8a8a",
  modalOverlay: "rgba(0, 0, 0, 0.35)",
  alertWarningBg: "#fdf6e3",
  alertWarningBorder: "#e8d5a0",
};

export function getColors(mode: ThemeMode): ThemeColors {
  return mode === "dark" ? DARK : LIGHT;
}

/* ------------------------------------------------------------------ */
/*  Shared constants                                                   */
/* ------------------------------------------------------------------ */

export const SIDEBAR_WIDTH = 240;
export const SIDEBAR_COLLAPSED_WIDTH = 64;
export const FONT_MONO =
  "'IBM Plex Mono', 'Fira Code', 'Fira Mono', 'Cascadia Code', 'Consolas', 'SF Mono', monospace";

/* ------------------------------------------------------------------ */
/*  Reusable inline-style objects                                      */
/* ------------------------------------------------------------------ */

/** Style for <pre> code blocks (query text, plan text, etc.) */
export function getCodeBlockStyle(mode: ThemeMode): React.CSSProperties {
  const c = getColors(mode);
  return {
    background: c.bgCode,
    color: c.codeText,
    padding: 16,
    borderRadius: 4,
    border: `1px solid ${c.border}`,
    overflow: "auto",
    maxHeight: 400,
    fontSize: 13,
    fontFamily: FONT_MONO,
    lineHeight: 1.6,
  };
}

/* Keep a static dark export for backward compat (TV is the dark palette) */
export const TV = { ...DARK, sidebarWidth: SIDEBAR_WIDTH, sidebarCollapsedWidth: SIDEBAR_COLLAPSED_WIDTH, fontMono: FONT_MONO };

/* ------------------------------------------------------------------ */
/*  State / status color maps (accent colors are the same in both)     */
/* ------------------------------------------------------------------ */

export function getSessionStateColors(mode: ThemeMode): Record<string, string> {
  const c = getColors(mode);
  return {
    active: c.green,
    idle: c.textSecondary,
    "idle in transaction": c.orange,
    "idle in transaction (aborted)": c.red,
    fastpath: c.primary,
    disabled: c.textSecondary,
  };
}

export function getQueryStatusColors(mode: ThemeMode): Record<string, string> {
  const c = getColors(mode);
  return {
    start: c.primary,
    done: c.green,
    error: c.red,
    canceled: c.yellow,
    idle: c.textSecondary,
  };
}

/** Dashboard icon colors keyed by normalised session state. */
export function getDashboardStateIcons(mode: ThemeMode): Record<string, string> {
  const c = getColors(mode);
  return {
    active: c.green,
    idle: c.textSecondary,
    "idle in transaction": c.yellow,
  };
}

/* Legacy static exports (dark mode) for files that don't use context yet */
export const sessionStateColors = getSessionStateColors("dark");
export const queryStatusColors = getQueryStatusColors("dark");
export const dashboardStateIcons = getDashboardStateIcons("dark");
export const codeBlockStyle = getCodeBlockStyle("dark");
