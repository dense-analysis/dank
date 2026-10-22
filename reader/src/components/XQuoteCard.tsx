import { articleParams } from "../lib/navigation";
import type { Post, XQuote } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { NavigableCard } from "./NavigableCard";
import { SourceActions } from "./SourceActions";
import { XPostContent } from "./XPostContent";
import { XPostHeader } from "./XPostHeader";

export interface XQuoteNavigation {
  params: URLSearchParams;
  openPost: (post: Pick<Post, "id" | "domain">) => void;
  selectAccount: (handle: string) => void;
}

export function XQuoteCard({
  quote,
  navigation,
}: {
  quote: XQuote;
  navigation: XQuoteNavigation;
}) {
  const post = quote.post;
  const destination =
    quote.collected && post
      ? {
          href: `/reader/?${articleParams(navigation.params, quote)}`,
          open: () => navigation.openPost(quote),
        }
      : undefined;

  return (
    <NavigableCard className="x-quote-card">
      <span className="visually-hidden">Quoted post</span>
      {post ? (
        <>
          <XPostHeader
            post={post}
            selectAccount={navigation.selectAccount}
            navigation={destination}
          />
          <XPostContent post={post} compact navigation={destination} />
        </>
      ) : (
        <p className="x-quote-unavailable">
          Quoted post unavailable in this capture.
        </p>
      )}
      {!destination && (
        <SourceActions
          className="x-quote-original"
          data-card-link
          href={safeUrl(quote.url)}
          target="_blank"
          rel="noreferrer"
        >
          View quoted post on X
        </SourceActions>
      )}
    </NavigableCard>
  );
}
