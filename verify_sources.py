#!/usr/bin/env python3
"""verify_sources.py — check every wire in this repository's source file and
report which answer, which are empty, and which are dead.

Recall depends more on wires that quietly 404 than on anything else in the
pipeline, and a dead feed looks identical to a quiet one in the feed itself.
Run this from the Actions tab whenever the feed looks thin.

    python3 verify_sources.py            # check everything
    python3 verify_sources.py --dead     # print only what to delete
"""
import argparse, importlib, os, sys
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
CANDIDATES = ["harvest_space", "harvest_env", "harvest_uap",
              "harvest_neo", "harvest_abortion", "harvest_invasion",
              "harvest"]


def load_harvester():
    sys.path.insert(0, HERE)
    for name in CANDIDATES:
        if os.path.exists(os.path.join(HERE, name + ".py")):
            return importlib.import_module(name)
    raise SystemExit("no harvester found next to this script")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dead", action="store_true", help="print only unreachable wires")
    args = ap.parse_args()

    h = load_harvester()
    sources, _cfg = h.load_sources()
    print("Checking %d wires…\n" % len(sources))

    def check(src):
        raw = h.fetch(src["url"], tries=1)
        if not raw:
            return src, "DEAD", 0
        try:
            items = h.parse_feed(raw, src)
        except Exception:                       # noqa: BLE001
            return src, "UNPARSEABLE", 0
        return src, ("EMPTY" if not items else "ok"), len(items)

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        for r in pool.map(check, sources):
            results.append(r)

    dead = [r for r in results if r[1] != "ok"]
    if not args.dead:
        for src, status, n in sorted(results, key=lambda r: (r[1] != "ok", r[0]["name"])):
            print("  %-46s %-12s %s" % (src["name"][:46], status, n or ""))
        print()
    print("%d of %d answered with items." % (len(results) - len(dead), len(results)))
    if dead:
        print("\nDelete or replace these in the source file:")
        for src, status, _n in dead:
            print("  %-46s %-12s %s" % (src["name"][:46], status, src["url"][:80]))


if __name__ == "__main__":
    main()
