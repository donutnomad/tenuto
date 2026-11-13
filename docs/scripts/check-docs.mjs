import fs from "node:fs"
import path from "node:path"
import process from "node:process"

const root = path.resolve(import.meta.dirname, "..")
const excludedNames = new Set(["RESULTS.md"])
const errors = []

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    if (["node_modules", ".vitepress", "internal", "examples", "skills"].includes(entry.name)) {
      return []
    }
    const fullPath = path.join(directory, entry.name)
    return entry.isDirectory() ? walk(fullPath) : [fullPath]
  })
}

function isPublished(file) {
  const name = path.basename(file)
  return file.endsWith(".md") && !name.startsWith("2026-") && !excludedNames.has(name)
}

function parseFrontmatter(source) {
  const match = source.match(/^---\n([\s\S]*?)\n---\n/)
  if (!match) return null
  const values = {}
  for (const line of match[1].split("\n")) {
    const separator = line.indexOf(":")
    if (separator < 0) continue
    values[line.slice(0, separator).trim()] = line.slice(separator + 1).trim().replace(/^['"]|['"]$/g, "")
  }
  return values
}

function routeFor(file) {
  const relative = path.relative(root, file).replaceAll(path.sep, "/").replace(/\.md$/, "")
  if (relative === "index") return "/"
  if (relative.endsWith("/index")) return `/${relative.slice(0, -"index".length)}`
  return `/${relative}`
}

const files = walk(root).filter(isPublished)
const routes = new Map(files.map((file) => [routeFor(file), file]))
const titles = new Map()
const referencedRoutes = new Set(["/"])

for (const file of files) {
  const relative = path.relative(root, file)
  const source = fs.readFileSync(file, "utf8")
  const frontmatter = parseFrontmatter(source)
  if (!frontmatter?.title || !frontmatter?.description) {
    errors.push(`${relative}: published pages require title and description frontmatter`)
  }
  if (frontmatter?.status && !["Stable", "Preview", "Internal"].includes(frontmatter.status)) {
    errors.push(`${relative}: status must be Stable, Preview, or Internal`)
  }
  if (frontmatter?.title) {
    const previous = titles.get(frontmatter.title)
    if (previous) errors.push(`${relative}: duplicate title also used by ${previous}`)
    titles.set(frontmatter.title, relative)
  }
  for (const match of source.matchAll(/\]\((\/[^)\s#]+)(?:#[^)\s]+)?\)/g)) {
    referencedRoutes.add(match[1])
  }
}

const config = fs.readFileSync(path.join(root, ".vitepress/config.mts"), "utf8")
for (const match of config.matchAll(/link:\s*["'](\/[^"'#]+)["']/g)) {
  referencedRoutes.add(match[1])
}

for (const route of referencedRoutes) {
  if (route === "/") continue
  const normalized = route.endsWith("/") ? route : route.replace(/\/$/, "")
  if (!routes.has(normalized) && !routes.has(`${normalized}/`)) {
    errors.push(`broken internal route: ${route}`)
  }
}

for (const [route, file] of routes) {
  if (route === "/" || referencedRoutes.has(route) || referencedRoutes.has(route.replace(/\/$/, ""))) continue
  errors.push(`${path.relative(root, file)}: page is absent from navigation and internal links`)
}

if (errors.length > 0) {
  console.error(errors.map((error) => `- ${error}`).join("\n"))
  process.exit(1)
}

console.log(`checked ${files.length} published documentation pages`)
