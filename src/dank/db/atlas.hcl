# Run through dank.db.atlas, which sets these variables for Atlas alone, so
# the password stays out of this file and the process list.
variable "url" {
  type    = string
  default = getenv("DANK_ATLAS_URL")
}

variable "dev_url" {
  type    = string
  default = getenv("DANK_ATLAS_DEV_URL")
}

# Only diff reads this, so the image doesn't need the provider.
data "external_schema" "models" {
  program = ["atlas-provider-sqlalchemy", "--path", "models", "--dialect", "postgresql"]
}

env "dank" {
  url = var.url
  dev = var.dev_url
  src = data.external_schema.models.url

  migration {
    dir = "file://migrations"
  }
}
