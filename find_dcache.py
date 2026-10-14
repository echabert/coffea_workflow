#!/usr/bin/env python3
"""Recursively list files inside a dCache directory (IPHC: sbgdcache.in2p3.fr).

Uses the standalone PyXrootd client (the `xrootd` package pinned in
requirements.txt and installed into condaEnv/) -- NOT fsspec, because the
pinned fsspec_xrootd 0.5.2 is incompatible with fsspec 2026.2.0's
tokenized filesystem constructor.

Every directory is listed with `dirlist(path, flags=DirListFlags.STAT)`,
so the entry name, size and modification time all come back in a single
request per directory. Sub-directories are discovered on the client side
and listed in parallel by a small worker pool (one XRootD FileSystem per
thread), which is much faster than a purely recursive client.

The default dCache host is sbgdcache.in2p3.fr, the same prefix used by
`load_file()` in plot_slurm.py, so a path given WITHOUT a `root://` prefix
(e.g. `/cms/phedex//store/user/...`) is automatically prefixed with
`root://sbgdcache.in2p3.fr//`.

Output written by `--out FILE` is one full `root://...` URL per line, which
is exactly the format `load_file()` in plot_slurm.py expects (files.txt).

Notes
-----
* The login nodes are trusted by IP by the IPHC dCache, so no grid proxy is
  needed there. From SLURM compute nodes set the proxy explicitly, e.g.:
      python3.12 find_dcache.py <dir> --proxy /home-pbs/echabert/proxy/x509up
  (or export X509_USER_PROXY beforehand, the script respects it).
* dCache canonical URLs use double slashes between mount points, e.g.
  `root://sbgdcache.in2p3.fr//cms/phedex//store/user/...` -- they are kept
  as-is, do not "clean them up".

Example
-------
    python3.12 find_dcache.py \
        /cms/phedex//store/user/xmadre/SIM_MC_Part/DYJetsToLL_0J_MLL_400_800_TuneCP5_13TeV-amcatnloFXFX-pythia8/Analysis_DYJetsToLL_0J_MLL_400_800_CodeV1p2/ \
        --human --out files.txt
"""

import argparse
import os
import queue
import re
import sys
import threading
import time

DEFAULT_HOST = "sbgdcache.in2p3.fr"

# Default test directory requested with the script
DEFAULT_DIR = (
    "/cms/phedex//store/user/xmadre/SIM_MC_Part/DYJetsToLL_0J_MLL_400_800_"
    "TuneCP5_13TeV-amcatnloFXFX-pythia8/"
    "Analysis_DYJetsToLL_0J_MLL_400_800_CodeV1p2/"
)

# Per-directory listing is fine up to this many entries before we warn
BIG_DIR_WARNING = 10000


def make_arg_parser():
    parser = argparse.ArgumentParser(
        prog="find_dcache.py",
        description=(
            f"Recursively find all files in a directory on the dCache at "
            f"{DEFAULT_HOST} (default) or any root:// dCache host."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=DEFAULT_DIR,
        help=(
            "Directory to scan. Either a full dCache URL "
            "(root://host//cms/phedex//...) or a bare dCache path "
            "(/cms/phedex//...), which is prefixed with "
            f"root://{DEFAULT_HOST}// automatically."
        ),
    )
    parser.add_argument(
        "--pattern",
        help="Only report entries whose FULL path matches this regular expression.",
    )
    parser.add_argument(
        "--include-dirs",
        action="store_true",
        help="Also report directory entries (by default only files are reported).",
    )
    parser.add_argument(
        "--out",
        metavar="FILE",
        help=(
            "Write one root:// URL per line into FILE. The format matches "
            "files.txt used by load_file() in plot_slurm.py (with "
            "--include-dirs, directories get a trailing /)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop reporting after N entries (walk still completes).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        metavar="N",
        help="Number of parallel directory listings.",
    )
    parser.add_argument(
        "--no-sizes",
        action="store_true",
        help="Do not show sizes in the summary (listing output is unchanged).",
    )
    parser.add_argument(
        "--human",
        action="store_true",
        help="Show sizes in human-readable units in the summary.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        metavar="SEC",
        help="Timeout in seconds for each XRootD request.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="Retries per directory listing before giving up on it.",
    )
    parser.add_argument(
        "--proxy",
        metavar="FILE",
        default=os.environ.get("X509_USER_PROXY", ""),
        help="Grid proxy (x509up) to use. Needed from SLURM compute nodes; "
        "the login nodes are trusted by IP and need none. "
        "Defaults to $X509_USER_PROXY.",
    )
    return parser


def normalize_url(path):
    """Return (base_url, start_path, url_of).

    base_url   : root://host (used to open the XRootD FileSystem)
    start_path : dCache path to start the walk at, always starting with /
    url_of(p)  : build the full canonical root:// URL of a dCache path p
    """
    p = path.strip()
    if not p.startswith("root://"):
        host = DEFAULT_HOST
        p = "/" + p.strip("/") if p.strip("/") else ""
    else:
        rest = p[len("root://"):]
        host = rest.split("/", 1)[0]
        after = rest[len(host):]
        # dCache canonical paths use "//" between mount points; keep them as-is
        p = "/" + after.lstrip("/") if after else ""
    # strip any trailing slash for clean joining
    start_path = p.rstrip("/") or "/"

    def url_of(p2):
        # canonical dCache URL: double slash right after the host
        return f"root://{host}//{p2.lstrip('/')}" if p2 else f"root://{host}//"

    return f"root://{host}", start_path, url_of


def human_size(nbytes):
    """480899445 -> '459.0 MB'"""
    if nbytes is None:
        return "?"
    size = float(nbytes)
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def make_entry_lister(fs, retries, timeout):
    """Return list_dir(dir_path) -> (ok, entries) with retries.

    Entries are dicts: {name, path, is_dir, size, mtime}.
    """
    from XRootD.client import flags as xflags

    def list_dir(dir_path):
        last_error = "unknown error"
        for _ in range(max(1, retries)):
            try:
                status, entries = fs.dirlist(
                    dir_path, flags=xflags.DirListFlags.STAT, timeout=timeout
                )
            except Exception as exc:  # connection drops, timeouts, ...
                last_error = repr(exc)
                time.sleep(1.0)
                continue
            if status is not None and status.ok:
                out = []
                for entry in entries or []:
                    if entry.name in (".", ".."):
                        continue
                    stat = getattr(entry, "statinfo", None)
                    flags = stat.flags if stat is not None else 0
                    out.append(
                        {
                            "name": entry.name,
                            "path": dir_path.rstrip("/") + "/" + entry.name,
                            # flags are StatInfoFlags (not POSIX mode bits):
                            # 51 = dir, 48 = regular file, etc.
                            "is_dir": bool(flags & xflags.StatInfoFlags.IS_DIR),
                            "size": stat.size if stat is not None else None,
                            "mtime": getattr(stat, "modtime", None)
                            if stat is not None
                            else None,
                        }
                    )
                return True, out
            last_error = status.message if status is not None else "no status"
            time.sleep(1.0)
        print(f"  [error] giving up on {dir_path}: {last_error}",
              file=sys.stderr, flush=True)
        return False, []

    return list_dir


def main():
    args = make_arg_parser().parse_args()

    # The proxy must be in the environment before the XRootD client is
    # imported / used (login nodes: trusted by IP, no proxy needed).
    if args.proxy:
        os.environ["X509_USER_PROXY"] = args.proxy
        print(f"Using grid proxy: {args.proxy}", flush=True)

    from XRootD.client.filesystem import FileSystem

    base_url, start_path, url_of = normalize_url(args.path)
    rx = re.compile(args.pattern) if args.pattern else None

    print(f"Scanning: {url_of(start_path)}")
    if args.pattern:
        print(f"Filter  : {args.pattern}")
    print(flush=True)

    results = []
    results_lock = threading.Lock()
    errors = []
    errors_lock = threading.Lock()
    dirs_listed = 0
    dirs_lock = threading.Lock()
    reached_limit = [False]

    work = queue.Queue()
    work.put(start_path)

    def worker():
        nonlocal dirs_listed
        # one XRootD FileSystem per thread (not thread-safe to share)
        fs = FileSystem(base_url)
        list_dir = make_entry_lister(fs, args.retries, args.timeout)
        while True:
            try:
                d = work.get_nowait()
            except queue.Empty:
                return
            ok, entries = list_dir(d)
            with dirs_lock:
                dirs_listed += 1
            if not ok:
                with errors_lock:
                    errors.append(d)
                continue
            if len(entries) > BIG_DIR_WARNING:
                print(f"  [warn] big directory ({len(entries)} entries): {d}",
                      file=sys.stderr, flush=True)
            for entry in entries:
                if rx is not None and not rx.search(url_of(entry["path"])):
                    continue
                if reached_limit[0]:
                    continue
                if entry["is_dir"] and not args.include_dirs:
                    continue
                with results_lock:
                    results.append(entry)
                    if args.limit and len(results) >= args.limit:
                        reached_limit[0] = True
            # descend into sub-directories (client-side recursion)
            for entry in entries:
                if entry["is_dir"]:
                    work.put(entry["path"])

    t0 = time.time()
    threads = [threading.Thread(target=worker)
               for _ in range(max(1, args.workers))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # deterministic output order
    results.sort(key=lambda e: (e["path"].count("/"), e["path"]))

    n_files = sum(1 for e in results if not e["is_dir"])
    n_dirs = len(results) - n_files
    total_bytes = sum(e["size"] or 0 for e in results if not e["is_dir"])

    for entry in results:
        line = url_of(entry["path"])
        if entry["is_dir"]:
            line += "/"
        print(line, flush=True)

    if args.out:
        with open(args.out, "w") as f:
            for entry in results:
                line = url_of(entry["path"])
                if entry["is_dir"]:
                    line += "/"
                f.write(line + "\n")
        print(f"\nWrote {len(results)} entries to: {args.out}", flush=True)

    elapsed = time.time() - t0
    print(f"\n=== Summary ===", flush=True)
    print(f"directories listed : {dirs_listed}", flush=True)
    print(f"files found        : {n_files}", flush=True)
    if args.include_dirs:
        print(f"sub-directories    : {n_dirs}", flush=True)
    if not args.no_sizes:
        size_str = f"{total_bytes} bytes"
        if args.human:
            size_str += f" ({human_size(total_bytes)})"
        print(f"total file size    : {size_str}", flush=True)
    print(f"listing errors     : {len(errors)}", flush=True)
    for e in errors[:10]:
        print(f"   {e}", flush=True)
    print(f"elapsed            : {elapsed:.1f} s", flush=True)

    sys.exit(1 if errors and not results else 0)


if __name__ == "__main__":
    main()