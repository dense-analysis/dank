# DANK reader development

- Read README.md. This is DANK’s new reader, developed separately from the
  legacy templates within this repository. Keep its branding, code and
  development artifacts under DANK.
- Use TypeScript strict mode, React components and plain CSS. Keep product state
  explicit; URL filters, query cache and local library each have one authority.
- Keep the HTTP contract in src/lib and source identity as (domain, post id).
- Render collected HTML only through the article sanitizer. Do not add remote
  scripts, embed scraped iframes or silently replace API errors with demo data.
- Keep presentation consistent across sources even when their collection
  methods differ. Reuse saved content and shared display rules for equivalent
  previews; preserve source identity and make unavailable content explicit.
- Keep named feeds as queries over collected content, distinct from source
  collection settings. Briefings and delivery require real backend support.
- Run npm run check and npm run test:e2e for behavioural changes. Run the root
  Python checks for API changes. Check desktop and mobile after layout edits.

## UI ownership

- Use React components, JSX, and component state for interactive controls and
  their feedback. Keep browser subscriptions and other external effects scoped
  to the component lifetime, with cleanup.
- Collected article HTML is a special boundary: the adapter sanitizes and wraps
  imported nodes, then provides stable hosts for React portals. React owns the
  controls inside those hosts. Avoid having the adapter and React both manage
  the same children; effects may synchronize presentation on imported nodes.
- Preserve article nodes when only preferences or control state change. When
  refactoring this boundary, check manual overrides, focus/scroll preservation,
  and pending async work during article replacement or unmount.

Examples to follow: [imageAnalysis.ts](src/lib/imageAnalysis.ts) separates sourced
contrast formulas from named heuristic thresholds; [article.ts](src/lib/article.ts)
owns the HTML adapter; [ArticleBody.tsx](src/components/ArticleBody.tsx) places
[ImageBackgroundToggle](src/components/ImageBackgroundToggle.tsx) and
[CodeCopyButton](src/components/CodeCopyButton.tsx) through portals. This is an
ownership pattern to reuse where it fits, not a required file split for every feature.
