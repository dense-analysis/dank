import { apiGet, ReaderRequestError } from "./api";
import { matchesLinkUrl } from "./linkIdentity";
import type { PostConnectionEvidence } from "./postConnections";
import type { ReferenceTarget } from "./referenceFocus";

interface ArchivedReference {
  excerpt: string;
  reference_url: string;
  matched_url: string;
}

/** Exact lookup is independent of the graph's bounded neighbourhood. */
export async function fetchReferenceEvidence(
  domain: string,
  postId: string,
  target: ReferenceTarget,
  signal?: AbortSignal,
): Promise<ArchivedReference | null> {
  const params = new URLSearchParams({
    domain,
    id: postId,
    target: target.url,
    kind: target.kind,
    reference_id: target.id,
  });

  if (target.saved) params.set("observed_at", target.saved);

  let evidence: PostConnectionEvidence;

  try {
    ({ evidence } = await apiGet("/api/reader/connection", {
      query: params,
      signal,
    }));
  } catch (error) {
    if (error instanceof ReaderRequestError && error.status === 404)
      return null;
    throw error;
  }

  // The archive must hold the exact reference the page asked about.
  if (
    evidence.domain !== domain ||
    evidence.post_id !== postId ||
    evidence.kind !== target.kind ||
    evidence.reference_id !== target.id ||
    !matchesLinkUrl(evidence.matched_url, target.url) ||
    !Number.isFinite(Date.parse(evidence.observed_at)) ||
    (target.saved &&
      Date.parse(evidence.observed_at) !== Date.parse(target.saved))
  )
    throw new Error("The saved reference could not be verified.");

  return {
    excerpt: evidence.excerpt,
    reference_url: evidence.reference_url,
    matched_url: evidence.matched_url,
  };
}
