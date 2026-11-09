-- Create "sources" table
CREATE TABLE "sources" (
  "id" uuid NOT NULL,
  "position" bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  "enabled" boolean NOT NULL,
  "config" jsonb NOT NULL,
  CONSTRAINT "pk_sources" PRIMARY KEY ("id")
);
-- Create index "uq_sources_config" to table: "sources"
CREATE UNIQUE INDEX "uq_sources_config" ON "sources" ((md5((config)::text)));
