import { ArrowLeft, Menu } from "lucide-react";
import type { ReactNode } from "react";

export function CollectionHeader({
  title,
  titleContent = title,
  avatar,
  handle,
  count,
  back,
  pageHeading,
  openNavigation,
}: {
  title: string;
  titleContent?: ReactNode;
  avatar?: ReactNode;
  handle?: string;
  count?: ReactNode;
  back?: { label: string; onClick: () => void };
  pageHeading: boolean;
  openNavigation: () => void;
}) {
  const Title = pageHeading ? "h1" : "span";

  return (
    <header className="topbar">
      <button
        type="button"
        className="icon-button menu-button"
        onClick={openNavigation}
        aria-label="Open navigation"
      >
        <Menu size={21} />
      </button>
      {back && (
        <button
          type="button"
          className="icon-button collection-back"
          onClick={back.onClick}
          aria-label={back.label}
        >
          <ArrowLeft size={20} />
        </button>
      )}
      <div className="breadcrumb">
        {avatar}
        <Title className="collection-title" title={title}>
          {titleContent}
        </Title>
        {handle && (
          <span className="collection-handle" title={handle}>
            {handle}
          </span>
        )}
      </div>
      {count}
    </header>
  );
}
