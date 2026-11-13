import type { components, paths } from "./api.gen";
import { filterParams } from "./navigation";
import type {
  Filters,
  Post,
  PostPage,
  SourceSettings,
  SourcesPage,
} from "./types";
import { isRecord } from "./validation";

export type FieldError = components["schemas"]["FieldError"];

/**
 * A request the server or the network refused. `code` and `fields` come from
 * the API's error envelope, so a form can match on the code and show each
 * field's message beside its input.
 */
export class ReaderRequestError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly code = "",
    public readonly fields: readonly FieldError[] = [],
  ) {
    super(message);
    this.name = "ReaderRequestError";
  }

  /** The message for one input, such as `body.domain` or `query.limit`. */
  fieldMessage(path: string): string | undefined {
    return this.fields.find((field) => field.path === path)?.message;
  }
}

type Method = "get" | "post" | "put";
type ApiPath = keyof paths;
type Operation<P extends ApiPath, M extends Method> = NonNullable<paths[P][M]>;

/** The paths that answer a method, so a call can't name a missing route. */
export type PathFor<M extends Method> = {
  [P in ApiPath]: [NonNullable<paths[P][M]>] extends [never] ? never : P;
}[ApiPath];

type JsonContent<R> = R extends { content: { "application/json": infer B } }
  ? B
  : never;

/** What a route answers on success, from the schema. */
export type ApiResponse<P extends ApiPath, M extends Method = "get"> =
  Operation<P, M> extends { responses: infer R }
    ? JsonContent<R[Extract<keyof R, 200 | 201 | 202>]>
    : never;

type PathParams<O> = O extends { parameters: { path: infer T } } ? T : never;
type RequestBody<O> = O extends {
  requestBody: { content: { "application/json": infer B } };
}
  ? B
  : never;

interface RequestOptions<O> {
  /** Values for the path's `{name}` segments. */
  params?: PathParams<O>;
  query?: URLSearchParams;
  signal?: AbortSignal;
}

export function apiGet<P extends PathFor<"get">>(
  path: P,
  options: RequestOptions<Operation<P, "get">> = {},
): Promise<ApiResponse<P, "get">> {
  return send(path, "GET", options) as Promise<ApiResponse<P, "get">>;
}

export function apiPost<P extends PathFor<"post">>(
  path: P,
  body: RequestBody<Operation<P, "post">>,
  options: RequestOptions<Operation<P, "post">> = {},
): Promise<ApiResponse<P, "post">> {
  return send(path, "POST", options, body) as Promise<ApiResponse<P, "post">>;
}

export function apiPut<P extends PathFor<"put">>(
  path: P,
  body: RequestBody<Operation<P, "put">>,
  options: RequestOptions<Operation<P, "put">> = {},
): Promise<ApiResponse<P, "put">> {
  return send(path, "PUT", options, body) as Promise<ApiResponse<P, "put">>;
}

export function apiUrl(
  path: string,
  params: Record<string, string> = {},
  query?: URLSearchParams,
): string {
  const filled = path.replace(/\{(\w+)\}/g, (_, name: string) => {
    const value = params[name];
    if (value === undefined) throw new Error(`Missing path value: ${name}`);

    return encodeURIComponent(value);
  });
  const search = query?.toString();

  return search ? `${filled}?${search}` : filled;
}

async function send(
  path: string,
  method: string,
  options: RequestOptions<unknown>,
  body?: unknown,
): Promise<unknown> {
  const headers = new Headers({ Accept: "application/json" });
  if (body !== undefined) headers.set("Content-Type", "application/json");

  const response = await fetch(
    apiUrl(
      path,
      options.params as Record<string, string> | undefined,
      options.query,
    ),
    {
      method,
      headers,
      signal: options.signal,
      body: body === undefined ? undefined : JSON.stringify(body),
    },
  );

  if (!response.ok) throw await requestError(response);

  try {
    // The server validates every answer against the schema these types come
    // from, so the body needs no second check here.
    return await response.json();
  } catch {
    throw new Error(
      "The reader received an unreadable response. Please try again.",
    );
  }
}

/** Reads the error envelope, the one place the reader does. */
async function requestError(response: Response): Promise<ReaderRequestError> {
  const fallback = `The reader could not load this request (${response.status}). Please try again.`;

  try {
    const body: unknown = await response.json();

    // A proxy may answer instead of DANK, so this body is checked.
    if (isRecord(body) && typeof body.error === "string") {
      const fields = Array.isArray(body.fields)
        ? body.fields.filter(
            (field): field is FieldError =>
              isRecord(field) &&
              typeof field.path === "string" &&
              typeof field.code === "string" &&
              typeof field.message === "string",
          )
        : [];

      return new ReaderRequestError(
        body.error.slice(0, 300) || fallback,
        response.status,
        typeof body.code === "string" ? body.code : "",
        fields,
      );
    }
  } catch {
    // A proxy may return HTML instead of the API's JSON error body.
  }

  return new ReaderRequestError(fallback, response.status);
}

export function fetchSources(signal?: AbortSignal): Promise<SourcesPage> {
  return apiGet("/api/reader/sources", { signal });
}

/** Every saved source, disabled ones too. */
export function fetchSourceSettings(
  signal?: AbortSignal,
): Promise<SourceSettings> {
  return apiGet("/api/reader/source-settings", { signal });
}

export function postParams(filters: Filters, limit = "30"): URLSearchParams {
  const params = filterParams(filters);
  params.delete("view");
  params.delete("x_tab");
  params.set("kind", filters.kind);
  if (filters.q.trim()) params.set("mode", "combined");
  // The API defaults to chronological order; the reader defaults search to relevance.
  if (filters.q.trim() && filters.sort === "relevance")
    params.set("sort", "relevance");
  params.set("limit", limit);

  return params;
}

export function fetchPosts(
  filters: Filters,
  cursor?: string | null,
  signal?: AbortSignal,
): Promise<PostPage> {
  const query = postParams(filters);
  if (cursor) query.set("cursor", cursor);

  return apiGet("/api/reader/posts", { query, signal });
}

export function fetchPost(
  domain: string,
  id: string,
  signal?: AbortSignal,
): Promise<Post> {
  const query = new URLSearchParams({ domain, id });

  return apiGet("/api/reader/post", { query, signal });
}

export async function fetchCategories(signal?: AbortSignal): Promise<string[]> {
  const body = await apiGet("/api/reader/categories", { signal });

  return body.categories;
}
