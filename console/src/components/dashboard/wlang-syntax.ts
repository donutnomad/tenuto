export type WlangTokenKind =
  | "plain"
  | "comment"
  | "keyword"
  | "namespace"
  | "function"
  | "type"
  | "string"
  | "number"
  | "constant"
  | "operator"

export interface WlangToken {
  text: string
  kind: WlangTokenKind
}

const KEYWORDS = new Set([
  "import", "let", "return", "if", "else", "foreach", "for", "step",
  "break", "continue", "panic", "defer", "select", "recv", "send",
  "default", "routine", "fn", "zero", "symbol", "method", "call", "pkg", "await",
  "struct", "match", "when",
])

const TYPES = new Set([
  "any", "bool", "byte", "error", "float32", "float64", "int", "int32",
  "int64", "map", "string", "uint", "uint32", "uint64", "array", "chan",
])

const CONSTANTS = new Set(["true", "false", "null", "nil"])
const QUALIFIED_TYPES = new Set([
  "ActivityFuture", "ActivityOptions", "Channel", "ChildWorkflowFuture", "Context",
  "RetryPolicy", "Selector", "SignalFuture", "WaitGroup",
])
const TOKEN_PATTERN = /\/\/.*|"(?:\\.|[^"\\])*"|`[^`]*`|\b\d+(?:\.\d+)?\b|[A-Za-z_][\w-]*|:=|->|==|!=|<=|>=|&&|\|\||<-|\.\.\.|\s+|./g
const OPERATOR_PATTERN = /^(?::=|->|==|!=|<=|>=|&&|\|\||<-|\.\.\.|[+\-*/%=<>!&|.:,;()[\]{}])$/

export function tokenizeWlangLine(line: string): WlangToken[] {
  const parts = line.match(TOKEN_PATTERN) ?? []
  return parts.map((text, index) => ({ text, kind: classifyToken(parts, index) }))
}

function classifyToken(parts: string[], index: number): WlangTokenKind {
  const text = parts[index]
  if (/^\s+$/.test(text)) return "plain"
  if (text.startsWith("//")) return "comment"
  if (text.startsWith('"') || text.startsWith("`")) return "string"
  if (/^\d/.test(text)) return "number"
  if (KEYWORDS.has(text)) return "keyword"
  if (TYPES.has(text)) return "type"
  if (CONSTANTS.has(text)) return "constant"
  if (QUALIFIED_TYPES.has(text)) return "type"
  if (OPERATOR_PATTERN.test(text)) return "operator"

  const previous = previousSignificant(parts, index)
  const next = nextSignificant(parts, index)
  if (next === "." || text === "workflow") return "namespace"
  if (next === "(" || previous === "." || previous === "symbol") return "function"
  return "plain"
}

function previousSignificant(parts: string[], index: number): string | undefined {
  for (let i = index - 1; i >= 0; i--) {
    if (!/^\s+$/.test(parts[i])) return parts[i]
  }
}

function nextSignificant(parts: string[], index: number): string | undefined {
  for (let i = index + 1; i < parts.length; i++) {
    if (!/^\s+$/.test(parts[i])) return parts[i]
  }
}
