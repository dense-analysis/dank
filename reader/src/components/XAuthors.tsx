import { useInfiniteQuery } from "@tanstack/react-query";
import { ArrowDown, ArrowUpRight } from "lucide-react";
import { useEffect, useRef } from "react";
import { articleParams, filterParams } from "../lib/navigation";
import type { Filters, Post } from "../lib/types";
import { safeUrl } from "../lib/urls";
import { fetchXAuthors } from "../lib/xAuthors";
import { Count } from "./Count";
import { IdentityAvatar } from "./IdentityAvatar";
import { PostTime } from "./PostTime";
import { ReaderLink } from "./ReaderLink";
import "../x-authors.css";

export function XAuthors({
  filters,
  params,
  navigate,
  open,
}: {
  filters: Filters;
  params: URLSearchParams;
  navigate: (params: URLSearchParams) => void;
  open: (post: Post) => void;
}) {
  const query = useInfiniteQuery({
    queryKey: ["x-authors", filters],
    queryFn: ({ pageParam, signal }) =>
      fetchXAuthors(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  });
  const more = useRef<HTMLDivElement>(null);
  const { hasNextPage, isFetching, isError, fetchNextPage } = query;
  useEffect(() => {
    if (
      !more.current ||
      !hasNextPage ||
      isFetching ||
      isError ||
      typeof IntersectionObserver === "undefined"
    )
      return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          observer.disconnect();
          void fetchNextPage({ cancelRefetch: false });
        }
      },
      { rootMargin: "0px 0px 240px 0px" },
    );
    observer.observe(more.current);

    return () => observer.disconnect();
  }, [hasNextPage, isFetching, isError, fetchNextPage]);

  const authors = [
    ...new Map(
      (query.data?.pages.flatMap((page) => page.authors) ?? []).map(
        (author) => [author.handle, author],
      ),
    ).values(),
  ];
  const first = query.data?.pages[0];

  return (
    <div className="x-author-explorer">
      <div className="x-author-summary">
        <h2>
          {first ? (
            <>
              <Count value={first.total} /> authors in these recommendations
            </>
          ) : (
            "Authors"
          )}
        </h2>
        <p>
          Most recommended authors first, counted by distinct saved posts, with
          up to three recent recommendations each.
        </p>
        {!!first?.unattributed_posts && (
          <p>
            <Count value={first.unattributed_posts} />{" "}
            {first.unattributed_posts === 1 ? "post has" : "posts have"} no
            saved author identity.
          </p>
        )}
      </div>
      {query.isPending && <p role="status">Loading recommended authors…</p>}
      {query.isSuccess && !authors.length && (
        <div className="empty-state">
          <h2>No authors match</h2>
          <p>
            Try fewer filters, or refresh For You to collect recommendations.
          </p>
        </div>
      )}
      <div className="x-author-groups">
        {authors.map((author) => {
          const next = filterParams({
            ...filters,
            account: author.handle,
            xTab: "posts",
          });
          const href = `/reader/?${next}`;
          return (
            <section
              className="x-author-group"
              key={author.handle}
              aria-label={`@${author.handle}`}
            >
              <div className="x-author-header">
                <IdentityAvatar
                  name={author.name}
                  iconUrl={author.icon_url}
                  kind="account"
                />
                <div>
                  <h3>
                    <ReaderLink href={href} onNavigate={() => navigate(next)}>
                      {author.name}
                    </ReaderLink>
                  </h3>
                  <span>@{author.handle}</span>
                  <span>
                    <Count value={author.count} />{" "}
                    {author.count === 1 ? "recommendation" : "recommendations"}
                  </span>
                </div>
                <ReaderLink
                  className="icon-button"
                  href={href}
                  onNavigate={() => navigate(next)}
                  aria-label={`View @${author.handle} recommendations`}
                >
                  <ArrowUpRight size={17} />
                </ReaderLink>
              </div>
              <div className="x-author-examples">
                {author.posts.map((post) => {
                  const thumbnail = post.thumbnail
                    ? safeUrl(post.thumbnail)
                    : undefined;
                  return (
                    <ReaderLink
                      className={`x-author-example ${thumbnail ? "has-thumbnail" : ""}`}
                      key={`${post.domain}:${post.id}`}
                      href={`/reader/?${articleParams(params, post)}`}
                      onNavigate={() => open(post)}
                      preserveTextSelection
                    >
                      {thumbnail && (
                        <img
                          src={thumbnail}
                          alt=""
                          loading="lazy"
                          referrerPolicy="no-referrer"
                        />
                      )}
                      <span>
                        <strong>
                          {post.excerpt || post.title || "Open saved post"}
                        </strong>
                        <small>
                          <PostTime date={post.created_at} />
                        </small>
                      </span>
                    </ReaderLink>
                  );
                })}
              </div>
            </section>
          );
        })}
      </div>
      {query.isError && (
        <div className="error-banner" role="alert">
          <span>{query.error.message}</span>
          <button
            type="button"
            className="text-button"
            onClick={() =>
              void (query.isFetchNextPageError
                ? query.fetchNextPage({ cancelRefetch: false })
                : query.refetch())
            }
          >
            Retry authors
          </button>
        </div>
      )}
      {query.hasNextPage && (
        <div className="load-more" ref={more}>
          <button
            type="button"
            className="secondary-button"
            disabled={query.isFetching}
            onClick={() => void query.fetchNextPage({ cancelRefetch: false })}
          >
            {query.isFetchingNextPage ? "Loading authors…" : "More authors"}
            <ArrowDown size={15} />
          </button>
        </div>
      )}
    </div>
  );
}
