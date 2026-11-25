# neo4j-aura-move-instance

Move an Aura instance to another project: snapshot, export, download, upload.

An instance cannot change project: PATCH does not accept the project, and both
clone-at-creation and overwrite across projects are refused with
"Cloning across tenants is not allowed" / source-db-not-in-same-tenant.
So the data has to leave Aura and come back into a new instance.

THE ONE CALL THAT NEEDS THE EXPORT FLAG  -> see export_url() below
    GET {v1beta5}/instances/{id}/snapshots/{sid}?include_download_url=true
Everything else uses the documented v1 API. Without the flag this script stops
at that call and the whole path falls back to doing it by hand in the console.

Usage profile here: ONE export per instance, ONCE, during a project split.
Not a backup pipeline, not recurring.

Requirements:
  - Aura API credentials with access to BOTH projects (AURA_CLIENT_ID / _SECRET)
  - snapshot export enabled on the organisation
  - neo4j-admin 2025+ (5.20 cannot read .backup, 5.23 fails to authenticate)
  - public traffic allowed to the target region during the upload

Caveats the operator must know:
  - the target gets a NEW connection URI, every consumer must be repointed
  - downtime: writes after the snapshot are lost, stop writers first
  - the export URL is a signed URL: anyone holding it can download the database
  - `database upload` loads one named database, so the source `system` database
    is not carried over. Verified: a custom role created on the source does not
    appear on the target, which comes up with its own built-in and Aura-generated
    roles. Indexes and constraints do follow, they live in the database itself.
    If your SSO group mappings point to built-in roles only, there is nothing to
    do; custom roles and local users have to be recreated before repointing.

  python aura_move_instance.py --instance-id <id> \
      --target-project <project-id> --target-name <name> [--dry-run]

Start with --dry-run: it runs every check and stops before the snapshot, so a
missing permission costs you nothing instead of an outage halfway through.
