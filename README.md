# environment-feed

Environmental destruction worldwide, filtered for scale.

`harvest_env.py` runs every two hours in GitHub Actions, reads 61 wires in 25 languages, scores
every story for how far its finding reaches, drops everything below the line, tags what remains
by subject, and writes `wire_env.json`. `index.html` loads that file and renders it.

Nothing here rewrites a headline. Titles and snippets are the publishers' own, truncated but
never reworded, and every row keeps its original link. No model in the pipeline, no API key, no
paid service, no dependencies beyond the Python standard library.

## The point of this feed

A single-site population survey on one river is a real environmental story and it does not belong
here. "Freshwater species have declined 85% since 1970, threatening food supplies for 200 million
people" does. The difference is scale, and this harvester measures it rather than trusting the
source to.

Each story is scored on six signals:

| Signal | Worth | What it means |
|---|---|---|
| Global scope | 2 | worldwide, planetary, every continent, across the world |
| Regional scope | 1 | a basin, ocean, continent or belt — Amazon, Arctic, Sahel, Mediterranean |
| Systemic | 2 | tipping point, collapse, cascade, irreversible threshold, mass extinction, a global assessment |
| Magnitude | 1 | a percentage, an area, a tonnage, a population figure |
| Consequence | 1 | deaths, displacement, food or water security, livelihoods |
| Finding | 1 | a study, assessment or dataset rather than an incident |

Below **2** a story never enters the feed. Every row shows its pips and the signals it scored on,
so the judgement is visible rather than hidden.

The harvester also reports how many stories each wire produced that were dropped for being too
local. That number is usually larger than the number kept, which is the feed working.

## Files

| File | What it does |
|---|---|
| `harvest_env.py` | Reads every wire, filters, scores, tags, deduplicates, writes `wire_env.json`. Self-contained. |
| `sources_env.json` | The wire list, including the planet-scale searches. Edit to add, drop or retune a feed. |
| `wire_env.json` | The output the page reads. Rewritten by the Action; do not hand-edit. |
| `index.html` | The feed page. Self-contained, reads `wire_env.json` over HTTPS. |
| `env-feed-weebly-embed.html` | The same page wrapped for a Weebly Embed Code element. Regenerate after changing `index.html`. |
| `.github/workflows/harvest.yml` | The schedule, plus a manual run button in the Actions tab. |

## Setup

1. Push these files to the repository root.
2. Settings → Actions → General → Workflow permissions → **Read and write permissions**, save.
3. Actions tab → **Harvest the environment wire** → *Run workflow*. First run takes two to three
   minutes.
4. Settings → Pages → Source: **Deploy from a branch**, branch `main`, folder `/ (root)`. Pages
   serves `index.html` at the repository root; a file named anything else 404s at that address.
5. Confirm `https://raw.githubusercontent.com/WelcomeToYourGalaxy/environment-feed/main/wire_env.json`
   loads in a browser. That URL is what the page fetches.

If you fork or rename the repository, change `REPO` near the top of the feed script in
`index.html`.

## Sources

**Analysis** — Carbon Brief, Yale Environment 360, Inside Climate News, Climate Home News, DeSmog.

**Field press** — Mongabay (English, Spanish, Portuguese), The Guardian environment desk, Grist,
Dialogue Earth.

**Journals** — Nature news, ScienceDaily earth and ecology, Phys.org earth and environment.

**Institutional** — UNEP, World Resources Institute, IUCN, Copernicus.

**Regional press** — Google News editions in English (US, UK, India, Australia, South Africa,
Nigeria, Kenya), Spanish (Spain, Mexico), Portuguese, French, German, Italian, Dutch, Swedish,
Greek, Polish, Russian, Ukrainian, Turkish, Arabic, Hebrew, Persian, Hindi, Bengali, Indonesian,
Vietnamese, Thai, Japanese, Chinese (simplified and traditional), Korean, Swahili. Each query is
written in that language, not translated at read time.

**Planet scale** — eight searches phrased for findings regional desks rarely lead with: planetary
limits, global assessments, worldwide declines, human consequence, toxics at scale, who is
responsible, and two in French and Spanish.

## Regions

Every story is placed geographically, by the ground the finding concerns rather than the wire it
arrived on. Ten buckets: Africa, Middle East & North Africa, Asia, Europe, Latin America &
Caribbean, North America, Oceania, Arctic & Antarctic, Oceans & high seas, and No single region —
which holds both worldwide findings that name no particular country and anything else naming
nowhere. A study spanning two continents files under both. Each row prints its region beside the
outlet.

The separate **Wire** filter is provenance, a different question: analysis desks, field press,
journals, institutions, regional press, planet-scale searches. No geography appears in it.

The **Window** filter covers the last 24 hours, 7 days and 30 days, plus *Older than 30 days* for
the tail of the 45-day archive. That chip appears only once there is something in it.

## Eleven subjects

Climate & atmosphere, Forests & land, Oceans & fisheries, Freshwater, Biodiversity & extinction,
Pollution & toxics, Extraction & mining, Agriculture & food, Energy & fossil fuels, Health &
human cost, Law & accountability. Each story carries every subject it matches.

## Coverage is uneven, and the file says so

`wire_env.json` records what each wire returned, how much it dropped, or that it could not be
reached. The page prints all of it under *Sources & coverage*, zeros included. Expect Swahili,
Bengali and Persian to read near zero most days.

The scoring is mechanical — it reads words, not meaning. An important local story written without
scale language will be dropped, and a thin story dressed in scale language can slip through. The
per-row pips exist so you can see which it is at a glance.

## Running it locally

```bash
python3 harvest_env.py              # full run
python3 harvest_env.py --dry-run    # harvest and report, write nothing
python3 harvest_env.py --fixtures tests/
```

Python 3.9 or later.
