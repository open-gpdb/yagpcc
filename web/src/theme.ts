/**
 * Turbo Vision–inspired theme for YAGPCC.
 *
 * Classic Turbo Vision used bright cyan on deep blue with high-contrast
 * box-drawing borders.  This adaptation keeps the retro terminal aesthetic
 * but uses **softer, less contrasting** tones and muted border lines.
 */

/* ------------------------------------------------------------------ */
/*  Color palette                                                      */
/* ------------------------------------------------------------------ */

export const TV = {
  /* Backgrounds */
  bgMain: "#1e2a3a",
  bgSidebar: "#172231",
  bgCard: "#243447",
  bgCode: "#1a2535",
  bgHover: "#2d4156",
  bgSelected: "#2d4156",
  bgSidebarHover: "#253a4f",
  bgElevated: "#2a3b4f",

  /* Text */
  textPrimary: "#c8d6e5",
  textSecondary: "#8899aa",
  textHeading: "#e0e8f0",

  /* Accents */
  primary: "#5ba4cf",
  cyan: "#6ec6c6",
  green: "#6abf8a",
  yellow: "#d4a95a",
  red: "#cf6b6b",
  orange: "#d4915a",

  /* Borders */
  border: "#2d4156",
  borderActive: "#3d5a75",

  /* Misc */
  sidebarWidth: 240,
  sidebarCollapsedWidth: 64,
  fontMono:
    "'IBM Plex Mono', 'Fira Code', 'Fira Mono', 'Cascadia Code', 'Consolas', 'SF Mono', monospace",
} as const;

/* ------------------------------------------------------------------ */
/*  Reusable inline-style objects                                      */
/* ------------------------------------------------------------------ */

/** Style for <pre> code blocks (query text, plan text, etc.) */
export const codeBlockStyle: React.CSSProperties = {
  background: TV.bgCode,
  color: TV.cyan,
  padding: 16,
  borderRadius: 4,
  border: `1px solid ${TV.border}`,
  overflow: "auto",
  maxHeight: 400,
  fontSize: 13,
  fontFamily: TV.fontMono,
  lineHeight: 1.6,
};

/* ------------------------------------------------------------------ */
/*  State / status color maps (used by badge components)               */
/* ------------------------------------------------------------------ */

export const sessionStateColors: Record<string, string> = {
  active: TV.green,
  idle: TV.textSecondary,
  "idle in transaction": TV.orange,
  "idle in transaction (aborted)": TV.red,
  fastpath: TV.primary,
  disabled: TV.textSecondary,
};

export const queryStatusColors: Record<string, string> = {
  start: TV.primary,
  done: TV.green,
  error: TV.red,
  canceled: TV.yellow,
  idle: TV.textSecondary,
};

/** Dashboard icon colors keyed by normalised session state. */
export const dashboardStateIcons: Record<string, string> = {
  active: TV.green,
  idle: TV.textSecondary,
  "idle in transaction": TV.yellow,
};
