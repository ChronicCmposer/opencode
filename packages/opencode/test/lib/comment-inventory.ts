// Comment-purge inventory (D24).
//
// This module only records comments that still reference the implementation
// plan. It never edits source. The inventory is grouped by package so the purge
// can be executed later as package-scoped, comment-only commits.

export type CommentReferenceKind = "parallel-implement" | "plan-reference"

export interface CommentReference {
  readonly path: string
  readonly line: number
  readonly column: number
  readonly kind: CommentReferenceKind
  readonly text: string
}

export interface SourceFile {
  readonly path: string
  readonly content: string
}

const PARALLEL_IMPLEMENT_PATTERN = /\.parallel-implement/
const PLAN_REFERENCE_PATTERN = /\b(?:PLAN-V2|W\d+\.\d+|Wave \d+|Step \d+|Phase \d+(?:\.\d+)?|plan task|plan decision)\b/

/**
 * The comment portion of a line: everything after `//` or `/*`, or a whole
 * continued block-comment line that starts with `*`. Returns `undefined` when
 * the line carries no comment text.
 */
function commentPortion(line: string): string | undefined {
  const lineComment = line.indexOf("//")
  const blockComment = line.indexOf("/*")
  const startsBlock = line.trimStart().startsWith("*")
  const candidates = [lineComment, blockComment].filter((index) => index >= 0)
  if (candidates.length === 0) return startsBlock ? line : undefined
  const start = startsBlock ? 0 : Math.min(...candidates)
  return line.slice(start)
}

/** Classify one comment as a plan reference, or `undefined` when it mentions neither. */
function classify(text: string): CommentReferenceKind | undefined {
  if (PARALLEL_IMPLEMENT_PATTERN.test(text)) return "parallel-implement"
  if (PLAN_REFERENCE_PATTERN.test(text)) return "plan-reference"
  return undefined
}

/**
 * Inventory plan references in the given files. Line and column are 1-based;
 * results are sorted by path then position so the artifact is deterministic.
 */
export function inventoryCommentReferences(files: ReadonlyArray<SourceFile>): ReadonlyArray<CommentReference> {
  const references: CommentReference[] = []

  for (const file of files) {
    const lines = file.content.split("\n")
    for (const [index, line] of lines.entries()) {
      const comment = commentPortion(line)
      if (comment === undefined) continue
      const kind = classify(comment)
      if (kind === undefined) continue
      const marker = comment.search(PARALLEL_IMPLEMENT_PATTERN.test(comment) ? PARALLEL_IMPLEMENT_PATTERN : PLAN_REFERENCE_PATTERN)
      references.push({
        path: file.path,
        line: index + 1,
        column: marker + 1,
        kind,
        text: comment.trim(),
      })
    }
  }

  return references.sort((left, right) => {
    if (left.path !== right.path) return left.path.localeCompare(right.path)
    if (left.line !== right.line) return left.line - right.line
    return left.column - right.column
  })
}

/** The package a path belongs to (`packages/<name>`), or `(root)`. */
export function packageOf(path: string): string {
  const match = /^packages\/([^/]+)\//.exec(path)
  return match === null ? "(root)" : `packages/${match[1]}`
}

export interface PackageInventory {
  readonly package: string
  readonly count: number
  readonly references: ReadonlyArray<CommentReference>
}

/** Group references by package, preserving the deterministic reference order. */
export function inventoryByPackage(
  references: ReadonlyArray<CommentReference>,
): ReadonlyArray<PackageInventory> {
  const grouped = new Map<string, CommentReference[]>()
  for (const reference of references) {
    const bucket = grouped.get(packageOf(reference.path)) ?? []
    bucket.push(reference)
    grouped.set(packageOf(reference.path), bucket)
  }
  return [...grouped.entries()]
    .map(([packageName, entries]) => ({ package: packageName, count: entries.length, references: entries }))
    .sort((left, right) => left.package.localeCompare(right.package))
}
