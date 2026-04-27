"""
Inspect totals_debug.html and tell us why no track links were found.

Run from the same directory where totals_debug.html lives:
    python inspect_totals.py
"""

import re
from pathlib import Path
from selectolax.parser import HTMLParser

P = Path("totals_debug.html")
if not P.exists():
    raise SystemExit("totals_debug.html not found in this directory")

html = P.read_text(encoding="utf-8")
print(f"file size: {len(html):,} bytes")

# 1. Raw substring presence — does the string '/spotify/track/' appear at all?
n_substr = html.count("/spotify/track/")
print(f"raw substring '/spotify/track/' occurrences: {n_substr}")

# 2. Same case-insensitively
n_ci = len(re.findall(r"/spotify/track/", html, flags=re.I))
print(f"raw substring (case-insensitive): {n_ci}")

# 3. How many <a> tags total in the file (raw regex, before parsing)
n_a_raw = len(re.findall(r"<a[\s>]", html, flags=re.I))
print(f"raw <a tags in file: {n_a_raw}")

# 4. Sample the first 3 occurrences of '/spotify/track/' so we see the
#    surrounding markup verbatim.
print("\n--- first 3 surrounding contexts ---")
for m in list(re.finditer(r"/spotify/track/", html))[:3]:
    s = max(0, m.start() - 80)
    e = min(len(html), m.end() + 80)
    print(repr(html[s:e]))
    print("---")

# 5. Now parse with selectolax and ask: what does it see?
tree = HTMLParser(html)
tables = tree.css("table")
print(f"\nselectolax: {len(tables)} <table> elements")
for i, t in enumerate(tables):
    anchors = t.css("a")
    print(f"  table[{i}]: {len(t.css('tr'))} rows, {len(anchors)} <a> elements")
    # Show first 3 anchors in this table
    for j, a in enumerate(anchors[:3]):
        attrs = dict(a.attributes)
        text = a.text(strip=True)[:40]
        print(f"    a[{j}] attrs={attrs} text={text!r}")

# 6. Check if maybe the <a> tags live OUTSIDE any <table> (e.g., the HTML
#    is malformed and selectolax closed the table early).
all_anchors = tree.css("a")
track_anchors = [
    a for a in all_anchors
    if "/spotify/track/" in (a.attributes.get("href", "") or "")
]
print(f"\nselectolax: {len(all_anchors)} <a> total, "
      f"{len(track_anchors)} with href containing '/spotify/track/'")
if track_anchors:
    a = track_anchors[0]
    print("first track anchor:", dict(a.attributes), repr(a.text(strip=True)))
