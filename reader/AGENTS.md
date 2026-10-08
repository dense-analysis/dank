# DANK reader development

- Read README.md. This is DANK’s new reader, developed separately from the
  legacy templates within this repository. Keep its branding, code and
  development artifacts under DANK.
- Use TypeScript strict mode, React components and plain CSS. Keep product state
  explicit; URL filters, query cache and local library each have one authority.
- Keep the HTTP contract in src/lib and source identity as (domain, post id).
- Render collected HTML only through the article sanitizer. Do not add remote
  scripts, embed scraped iframes or silently replace API errors with demo data.
- Keep named feeds as queries over collected content, distinct from source
  collection settings. Briefings and delivery require real backend support.
- Run npm run check and npm run test:e2e for behavioural changes. Run the root
  Python checks for API changes. Check desktop and mobile after layout edits.
