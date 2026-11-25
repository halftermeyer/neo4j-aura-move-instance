#!/usr/bin/env python3
"""
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
"""

import argparse
import os
import shlex
import subprocess
import sys
import time

import requests

API = "https://api.neo4j.io/v1"
API_EXPORT = "https://api.neo4j.io/v1beta5"  # export only
AUTH = "https://api.neo4j.io/oauth/token"

MIN_BACKUP_BYTES = 1024  # anything smaller is not a backup

_tok = {"v": None, "exp": 0}


def token() -> str:
    if _tok["v"] and time.time() < _tok["exp"] - 60:
        return _tok["v"]
    try:
        cid, secret = os.environ["AURA_CLIENT_ID"], os.environ["AURA_CLIENT_SECRET"]
    except KeyError:
        sys.exit("set AURA_CLIENT_ID and AURA_CLIENT_SECRET in the environment")
    r = requests.post(AUTH, auth=(cid, secret), data={"grant_type": "client_credentials"})
    if r.status_code >= 400:
        sys.exit(f"authentication failed: {r.status_code}\n{r.text}")
    d = r.json()
    _tok["v"], _tok["exp"] = d["access_token"], time.time() + d.get("expires_in", 3600)
    return _tok["v"]


def api(method: str, path: str, base: str = API, fatal: bool = True, **kw):
    r = requests.request(
        method,
        f"{base}{path}",
        headers={"Authorization": f"Bearer {token()}", "Content-Type": "application/json"},
        **kw,
    )
    if r.status_code >= 400:
        if fatal:
            sys.exit(f"{method} {path} -> {r.status_code}\n{r.text}")
        return None
    return r.json().get("data")


# --------------------------------------------------------------------------
# preflight: everything that can be checked before anything is disturbed
# --------------------------------------------------------------------------

def preflight(args) -> dict:
    print("== preflight ==")

    # /tenants is known to under-report: it does not always list every project
    # you can actually create instances in. Names are shown when available,
    # and their absence means nothing.
    projects = {p["id"]: p.get("name", "") for p in (api("GET", "/tenants") or [])}

    src = api("GET", f"/instances/{args.instance_id}")
    src_project = src["tenant_id"]

    def label(pid):
        name = projects.get(pid)
        return f'{pid} "{name}"' if name else pid

    print(f"source : {src['id']} \"{src['name']}\" {src['type']} "
          f"{src['memory']}/{src['storage']} {src['cloud_provider']}/{src['region']} "
          f"status={src['status']}")
    print(f"         project {label(src_project)}")
    print(f"target : \"{args.target_name}\" in project {label(args.target_project)}")

    problems = []
    if src_project == args.target_project:
        problems.append("source and target projects are the same")
    if src["status"] != "running":
        problems.append(f"source instance is {src['status']}, expected running")

    # neo4j-admin: present, and recent enough
    try:
        out = subprocess.run([args.neo4j_admin, "--version"],
                             capture_output=True, text=True, timeout=120)
        version = (out.stdout + out.stderr).strip().splitlines()[-1]
        print(f"neo4j-admin: {version}")
        major = version.split(".")[0]
        if major.isdigit() and int(major) < 2025:
            problems.append(f"neo4j-admin {version} is too old, use 2025 or later "
                            "(5.20 cannot read .backup, 5.23 fails to authenticate)")
    except FileNotFoundError:
        problems.append(f"neo4j-admin not found at '{args.neo4j_admin}'")
    except Exception as e:  # noqa: BLE001
        problems.append(f"could not run neo4j-admin: {e}")

    if problems:
        print("\n== blocked ==")
        for p in problems:
            print(" -", p)
        sys.exit(1)

    print("preflight passed")
    return src


# --------------------------------------------------------------------------
# migration steps
# --------------------------------------------------------------------------

def wait(instance_id: str, timeout: int = 1800) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = api("GET", f"/instances/{instance_id}")
        print(f"  {instance_id}: {d['status']}")
        if d["status"] == "running":
            return d
        time.sleep(15)
    sys.exit(f"timeout waiting for {instance_id}")


def snapshot(instance_id: str) -> str:
    sid = api("POST", f"/instances/{instance_id}/snapshots")["snapshot_id"]
    print(f"snapshot {sid}")
    while True:
        d = api("GET", f"/instances/{instance_id}/snapshots/{sid}")
        print(f"  {d['status']}")
        if d["status"] in ("Completed", "Failed", "Cancelled"):
            break
        time.sleep(15)
    if d["status"] != "Completed":
        sys.exit(f"snapshot {d['status']}")
    if not d.get("exportable"):
        sys.exit("snapshot is not exportable")
    return sid


def export_url(instance_id: str, snapshot_id: str) -> str:
    """The flag-dependent call. v1 returns `exportable` but rejects the parameter."""
    d = api("GET", f"/instances/{instance_id}/snapshots/{snapshot_id}",
            base=API_EXPORT, params={"include_download_url": "true"})
    url = next((v for k, v in (d or {}).items() if "url" in k.lower() and v), None)
    if not url:
        sys.exit(
            "No export URL returned: the snapshot export flag is not enabled on this "
            "organisation. Everything above this point worked; this is the single "
            "capability the migration depends on."
        )
    return url


def download(url: str, work_dir: str, database: str) -> str:
    """File must be named after the database it will be loaded into."""
    os.makedirs(work_dir, exist_ok=True)
    path = os.path.join(work_dir, f"{database}.backup")

    r = requests.get(url, stream=True)
    if r.status_code >= 400:
        sys.exit(f"download failed: {r.status_code}\n{r.text[:500]}")

    ctype = r.headers.get("content-type", "")
    if ctype.startswith(("text/", "application/xml")):
        sys.exit(f"the export URL returned {ctype}, not a backup:\n{r.text[:500]}")

    size = 0
    with open(path, "wb") as f:
        for chunk in r.iter_content(1 << 20):
            f.write(chunk)
            size += len(chunk)

    if size < MIN_BACKUP_BYTES:
        with open(path, "rb") as f:
            head = f.read(400)
        sys.exit(f"downloaded file is only {size} bytes, this is not a backup:\n{head!r}")

    print(f"{path}: {size / 1e6:.1f} MB ({size} bytes)")
    return path


def create_target(src: dict, project_id: str, name: str) -> dict:
    d = api("POST", "/instances", json={
        "version": src.get("version", "5"),
        "region": src["region"],
        "memory": src["memory"],
        "name": name,
        "type": src["type"],
        "tenant_id": project_id,
        "cloud_provider": src["cloud_provider"],
    }, fatal=False)
    if d is None:
        sys.exit(f"could not create the target instance in project {project_id}.\n"
                 "A 403 here usually means a stale or mistyped project id rather than "
                 "missing permissions: check it against the console.\n"
                 "The snapshot was taken and is still available on the source instance.")
    print(f"target {d['id']} -> {d['connection_url']}")
    print("store the target password now, it is not retrievable later")
    wait(d["id"])
    return d


def upload(work_dir: str, database: str, uri: str, password: str, admin: str) -> None:
    """No --verbose: it prints the instance password in clear text."""
    cmd = (f"{shlex.quote(admin)} database upload {database} "
           f"--from-path={shlex.quote(work_dir)} "
           f"--to-uri={shlex.quote(uri)} "
           f"--to-user=neo4j "
           f"--to-password={shlex.quote(password)} "
           f"--overwrite-destination=true")
    print(cmd.replace(password, "********"))
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print((res.stdout + res.stderr).replace(password, "********")[-4000:])
    if res.returncode:
        sys.exit("upload failed. If it reports an unrecovered backup, run "
                 "`neo4j-admin database aggregate-backup --from-path=<dir>` and retry.")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--instance-id", required=True, help="instance to move")
    p.add_argument("--target-project", required=True, help="destination project id")
    p.add_argument("--target-name", required=True)
    p.add_argument("--database", default="neo4j")
    p.add_argument("--work-dir", default="aura-migration")
    p.add_argument("--neo4j-admin", default="neo4j-admin")
    p.add_argument("--dry-run", action="store_true",
                   help="run every check, then stop before the snapshot")
    p.add_argument("--yes", action="store_true",
                   help="skip the confirmation prompt (for batch runs)")
    args = p.parse_args()

    src = preflight(args)

    if args.dry_run:
        print("\ndry run: stopping here, nothing was changed")
        return

    print("\nStop or drain writers now: anything written after the snapshot is lost.")
    if not args.yes:
        input("press Enter to continue, Ctrl-C to abort ")

    sid = snapshot(args.instance_id)
    url = export_url(args.instance_id, sid)          # <- needs the export flag
    download(url, args.work_dir, args.database)

    tgt = create_target(src, args.target_project, args.target_name)
    upload(args.work_dir, args.database, tgt["connection_url"],
           tgt["password"], args.neo4j_admin)

    print(f"""
Done. Before touching the source instance:
  1. repoint every consumer to {tgt['connection_url']}
  2. reapply IP filtering, SSO link, custom endpoint, metrics and log forwarding
  3. recreate custom roles and local users: the source `system` database is not
     carried over, so SHOW ROLES on the target shows built-in roles only
     (indexes and constraints do follow)
  4. run your own data checks
  5. only then pause, and later delete, {args.instance_id}

Deleting an instance also deletes its snapshots, with no way to recover them.
Delete the local .backup file once you are satisfied: it is a full copy of the database.
""")


if __name__ == "__main__":
    main()
