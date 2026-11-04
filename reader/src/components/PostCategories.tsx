import type { Post } from "../lib/types";

export function PostCategories({ post }: { post: Post }) {
  if (!post.categories?.length) return null;

  return (
    <ul className="post-categories" aria-label="Post categories">
      {post.categories.map((category) => (
        <li key={category.name}>{category.name}</li>
      ))}
    </ul>
  );
}
