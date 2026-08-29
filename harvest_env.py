#!/usr/bin/env python3
"""
harvest_env.py — the environment wire: what is being destroyed, at what scale,
and what that is doing to people.

Self-contained: fetching, feed parsing, word-edge matching and deduplication are
all in this file. Reads sources_env.json, writes wire_env.json. Standard library
only — no dependencies, no API keys, no model calls.

The distinguishing feature of this feed is a scale score. An environmental story
is not kept because it is about the environment; it is kept because it carries a
finding at scale — a global or basin-wide scope, a magnitude, a systemic
mechanism, or a human consequence. A survey of water bugs in one Angolan river
scores nothing and never appears. "Freshwater species have declined 85% since
1970, threatening food supplies for 200 million people" scores on all four.

    python3 harvest_env.py
    python3 harvest_env.py --dry-run
    python3 harvest_env.py --fixtures DIR
"""

import argparse
import json
import os
import re
import sys
import time
import gzip
import html
import io
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES_PATH = os.path.join(HERE, "sources_env.json")
OUT_PATH = os.path.join(HERE, "wire_env.json")

RETAIN_DAYS = 45
MAX_ITEMS = 1200
WORKERS = 6
KEEP_SCORE = 2          # a story needs this much scale to enter the feed at all
BIG_PICTURE_SCORE = 4   # what the page's "big picture only" filter uses

# --------------------------------------------------------------------------
# Plumbing: fetching, feed parsing, word-edge matching, fingerprints.
# --------------------------------------------------------------------------
USER_AGENT = ("Mozilla/5.0 (compatible; space-life-news/1.0; "
              "+https://github.com/WelcomeToYourGalaxy/space-life-news)")

TIMEOUT = 25

SNIPPET_CHARS = 240

TAG_RE = re.compile(r"<[^>]+>")

WS_RE = re.compile(r"\s+")

PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)

def build_gnews_url(loc):
    q = loc["query"] + " when:30d"
    return ("https://news.google.com/rss/search?q=" + urllib.parse.quote(q) +
            "&hl=" + loc["hl"] + "&gl=" + loc["gl"] + "&ceid=" + loc["ceid"])

def fetch(url, tries=3):
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
                "Accept-Encoding": "gzip",
                "Accept-Language": "*",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return raw
        except Exception as exc:                       # noqa: BLE001 — report, don't crash the run
            last = exc
            time.sleep(1.5 * (attempt + 1))
    print("  ! unreachable: %s (%s)" % (url[:90], last), file=sys.stderr)
    return None

def strip_ns(tag):
    return tag.split("}", 1)[1] if "}" in tag else tag

def text_of(el):
    return WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", el.text or ""))).strip() if el is not None else ""

def child(node, *names):
    for kid in node:
        if strip_ns(kid.tag) in names:
            return kid
    return None

def parse_date(raw):
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None

def parse_feed(raw, src):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # Some publishers serve a stray byte before the declaration.
        try:
            root = ET.fromstring(raw[raw.index(b"<"):])
        except Exception:  # noqa: BLE001
            return []

    nodes = [n for n in root.iter() if strip_ns(n.tag) == "item"]
    atom = False
    if not nodes:
        nodes = [n for n in root.iter() if strip_ns(n.tag) == "entry"]
        atom = True

    out = []
    for n in nodes:
        title = text_of(child(n, "title"))
        if atom:
            link = ""
            for kid in n:
                if strip_ns(kid.tag) == "link" and kid.get("rel", "alternate") == "alternate":
                    link = kid.get("href", "")
                    break
        else:
            link_el = child(n, "link")
            link = (link_el.text or "").strip() if link_el is not None else ""
            if not link:
                link = text_of(child(n, "guid"))
        if not title or not link:
            continue

        outlet_el = child(n, "source")
        outlet = text_of(outlet_el) if outlet_el is not None else ""
        if outlet and title.endswith(" - " + outlet):
            title = title[: -(len(outlet) + 3)].strip()
        elif not outlet and src["name"].startswith("Google News") and " - " in title:
            # Google News appends the outlet to the headline when it omits <source>.
            head, _, tail = title.rpartition(" - ")
            if head and 2 <= len(tail) <= 45:
                title, outlet = head.strip(), tail.strip()

        stamp = parse_date(text_of(child(n, "pubDate", "published", "updated", "date")))
        snippet = text_of(child(n, "description", "summary", "content"))[:SNIPPET_CHARS]

        out.append({
            "t": title,
            "u": link,
            "o": outlet or src["name"].replace("Google News · ", ""),
            "g": src["lang"],
            "r": src["region"],
            "k": src.get("kind", "news"),
            "d": stamp,
            "s": snippet,
            "w": src["name"],
        })
    return out

def _compile(term):
    if any(ord(ch) > 0x24F for ch in term):        # non-Latin script
        return term
    if term.endswith("*"):
        return re.compile(r"(?<![a-z0-9])" + re.escape(term[:-1]) + r"[a-z0-9\-]*", re.I)
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])", re.I)

def _compile_all(terms):
    return [_compile(t) for t in terms]

def hit(text, compiled):
    """True when any compiled term matches."""
    for c in compiled:
        if isinstance(c, str):
            if c in text:
                return True
        elif c.search(text):
            return True
    return False

def fingerprint(title):
    norm = PUNCT_RE.sub(" ", title.lower())
    return " ".join(WS_RE.sub(" ", norm).strip().split()[:9])

def canon_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parts.query)
        query = [(k, v) for k, v in query if not k.lower().startswith("utm_")]
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"),
                                        urllib.parse.urlencode(query), ""))
    except Exception:  # noqa: BLE001
        return url


# --------------------------------------------------------------------------
# Subjects
# --------------------------------------------------------------------------
TOPICS = [
    ("climate", "Climate & atmosphere", [
        ("climate change", None), ("global warming", None), ("greenhouse gas*", None),
        ("carbon emission*", None), ("co2 emission*", None), ("methane emission*", None),
        ("heatwave*", None), ("heat wave*", None), ("record temperature*", None),
        ("sea level rise", None), ("glacier*", None), ("permafrost", None), ("ice sheet", None),
        ("sea ice", None), ("el niño", None), ("la niña", None), ("carbon budget", None),
        ("changement climatique", None), ("cambio climático", None), ("mudança climática", None),
        ("klimawandel", None), ("cambiamento climatico", None), ("изменение климата", None),
        ("气候变化", None), ("氣候變遷", None), ("気候変動", None), ("기후변화", None),
        ("تغير المناخ", None), ("जलवायु परिवर्तन", None), ("perubahan iklim", None),
        ("iklim değişikliği", None), ("klimaatverandering", None), ("zmiana klimatu", None),
    ]),
    ("forest", "Forests & land", [
        ("deforestation", None), ("forest loss", None), ("forest degradation", None),
        ("logging", None), ("land clearing", None), ("land-use change", None),
        ("desertification", None), ("soil degradation", None), ("soil erosion", None),
        ("peatland*", None), ("mangrove*", None), ("savanna*", ["loss", "clearing", "degrad", "fire"]),
        ("amazon", ["deforest", "forest", "fire", "clearing", "rainforest", "basin"]),
        ("congo basin", None), ("boreal forest", None), ("rainforest", None),
        ("déforestation", None), ("deforestación", None), ("desmatamento", None),
        ("entwaldung", None), ("deforestazione", None), ("вырубка лес", None),
        ("森林砍伐", None), ("森林減少", None), ("산림 파괴", None), ("إزالة الغابات", None),
        ("वनों की कटाई", None), ("deforestasi", None), ("ontbossing", None), ("wylesianie", None),
    ]),
    ("ocean", "Oceans & fisheries", [
        ("ocean warming", None), ("ocean acidification", None), ("marine heatwave*", None),
        ("coral bleaching", None), ("coral reef*", ["loss", "bleach", "die", "decline", "damage"]),
        ("overfishing", None), ("bottom trawling", None), ("fish stock*", ["collapse", "decline", "overfish"]),
        ("deep-sea mining", None), ("deep sea mining", None), ("dead zone*", ["ocean", "sea", "coastal", "hypox"]),
        ("plastic in the ocean", None), ("ocean plastic", None), ("krill", ["decline", "fishing", "collapse"]),
        ("acidification des océans", None), ("acidificación", None), ("acidificação", None),
        ("meeresversauerung", None), ("海洋酸化", None), ("海洋酸化", None), ("해양 산성화", None),
        ("surpêche", None), ("sobrepesca", None), ("überfischung", None), ("过度捕捞", None),
    ]),
    ("freshwater", "Freshwater", [
        ("groundwater depletion", None), ("aquifer*", ["depleted", "depletion", "contaminat", "overdraft"]),
        ("water scarcity", None), ("water crisis", None), ("drought", None),
        ("river*", ["dried", "drying", "pollut", "contaminat", "decline", "flow", "dammed"]),
        ("wetland*", ["loss", "drained", "destroy", "decline"]), ("lake*", ["shrink", "drying", "dead", "pollut"]),
        ("freshwater species", None), ("dam*", ["river", "displac", "flow", "sediment", "fish"]),
        ("glacier melt", None), ("snowpack", ["decline", "record low", "loss"]),
        ("pénurie d'eau", None), ("escasez de agua", None), ("escassez de água", None),
        ("wasserknappheit", None), ("нехватка воды", None), ("水危机", None), ("水不足", None),
        ("물 부족", None), ("ندرة المياه", None), ("जल संकट", None), ("krisis air", None),
    ]),
    ("biodiversity", "Biodiversity & extinction", [
        ("biodiversity loss", None), ("mass extinction", None), ("extinction risk", None),
        ("species decline", None), ("population decline", ["species", "wildlife", "bird", "insect", "fish"]),
        ("insect decline", None), ("pollinator*", ["decline", "loss", "collapse"]),
        ("habitat loss", None), ("wildlife trade", None), ("poaching", None),
        ("red list", None), ("living planet index", None), ("defaunation", None),
        ("perte de biodiversité", None), ("pérdida de biodiversidad", None),
        ("perda de biodiversidade", None), ("artensterben", None), ("biodiversitätsverlust", None),
        ("perdita di biodiversità", None), ("утрата биоразнообразия", None),
        ("生物多样性丧失", None), ("生物多樣性喪失", None), ("生物多様性の損失", None),
        ("생물다양성", ["감소", "손실", "위기"]), ("فقدان التنوع البيولوجي", None),
        ("जैव विविधता", ["नुकसान", "हानि", "संकट"]), ("keanekaragaman hayati", ["hilang", "kehilangan"]),
    ]),
    ("pollution", "Pollution & toxics", [
        ("air pollution", None), ("particulate matter", None), ("pm2.5", None),
        ("water pollution", None), ("plastic pollution", None), ("microplastic*", None),
        ("nanoplastic*", None), ("pfas", None), ("forever chemical*", None),
        ("pesticide*", ["residue", "exposure", "ban", "contaminat", "decline", "poison"]),
        ("heavy metal*", ["contaminat", "exposure", "poison", "soil", "water"]),
        ("mercury", ["contaminat", "poison", "mining", "fish", "exposure"]),
        ("lead poisoning", None), ("toxic waste", None), ("oil spill", None), ("tailings", None),
        ("smog", None), ("nitrogen pollution", None), ("eutrophication", None),
        ("pollution de l'air", None), ("contaminación del aire", None), ("poluição do ar", None),
        ("luftverschmutzung", None), ("inquinamento", None), ("загрязнение", None),
        ("空气污染", None), ("空氣污染", None), ("大気汚染", None), ("대기오염", None),
        ("تلوث الهواء", None), ("वायु प्रदूषण", None), ("polusi udara", None), ("hava kirliliği", None),
    ]),
    ("extraction", "Extraction & mining", [
        ("open-pit mine", None), ("open pit mining", None), ("strip mining", None),
        ("mining concession*", None), ("illegal mining", None), ("gold mining", ["mercury", "amazon", "illegal", "river"]),
        ("lithium mining", None), ("cobalt mining", None), ("nickel mining", None),
        ("sand mining", None), ("oil drilling", None), ("gas flaring", None), ("fracking", None),
        ("tar sands", None), ("pipeline", ["spill", "leak", "protest", "approval", "route", "oil", "gas"]),
        ("garimpo", None), ("minería", ["contaminación", "ilegal", "río", "concesión"]),
        ("bergbau", ["umwelt", "verschmutzung", "abbau"]), ("добыча", ["нефт", "загрязн", "шахт"]),
        ("采矿", ["污染", "生态", "非法"]), ("鉱山", ["汚染", "環境"]),
    ]),
    ("food", "Agriculture & food", [
        ("industrial agriculture", None), ("monoculture", None), ("livestock emissions", None),
        ("cattle ranching", ["deforest", "amazon", "clearing"]), ("soy expansion", None),
        ("palm oil", ["deforest", "clearing", "plantation", "expansion"]),
        ("fertiliser runoff", None), ("fertilizer runoff", None), ("crop failure*", None),
        ("food security", None), ("harvest loss*", None), ("yield decline", None), ("famine", None),
        ("sécurité alimentaire", None), ("seguridad alimentaria", None), ("segurança alimentar", None),
        ("ernährungssicherheit", None), ("продовольственная безопасность", None),
        ("粮食安全", None), ("食料安全保障", None), ("식량 안보", None), ("الأمن الغذائي", None),
    ]),
    ("energy", "Energy & fossil fuels", [
        ("fossil fuel*", None), ("coal plant*", None), ("coal expansion", None),
        ("oil and gas expansion", None), ("lng terminal*", None), ("carbon capture", None),
        ("net zero", None), ("energy transition", None), ("renewable", ["expansion", "record", "capacity", "transition"]),
        ("subsid*", ["fossil", "coal", "oil", "gas"]),
        ("combustibles fósiles", None), ("combustíveis fósseis", None), ("énergies fossiles", None),
        ("fossile brennstoffe", None), ("化石燃料", None), ("화석연료", None), ("ископаемое топливо", None),
    ]),
    ("health", "Health & human cost", [
        ("premature death*", None), ("excess death*", None), ("mortality", ["pollution", "heat", "climate", "smoke", "toxic"]),
        ("public health", ["pollution", "climate", "toxic", "contamination", "heat"]),
        ("cancer risk", ["pollution", "chemical", "contamination", "pesticide"]),
        ("respiratory illness", None), ("waterborne disease", None), ("malnutrition", None),
        ("displacement", ["climate", "flood", "drought", "disaster", "sea level"]),
        ("climate refugee*", None), ("heat deaths", None), ("children exposed", None),
        ("muertes prematuras", None), ("mortes prematuras", None), ("décès prématurés", None),
        ("vorzeitige todesfälle", None), ("过早死亡", None), ("早死", None),
    ]),
    ("accountability", "Law & accountability", [
        ("lawsuit", ["climate", "pollution", "environment", "contamination", "emissions", "mining", "spill"]),
        ("court ruling", ["climate", "environment", "pollution", "emissions"]),
        ("ecocide", None), ("carbon majors", None), ("greenwashing", None),
        ("regulation rollback", None), ("deregulat*", ["environment", "climate", "pollution", "emissions"]),
        ("cop30", None), ("cop29", None), ("unfccc", None), ("plastics treaty", None),
        ("environmental defender*", None), ("land defender*", None),
        ("indigenous land", ["mining", "logging", "oil", "dam", "deforest", "titled", "protect"]),
        ("supply chain", ["deforest", "child labour", "traceab", "due diligence"]),
        ("fined", ["pollution", "emissions", "environmental", "spill", "contamination"]),
        ("procès climatique", None), ("demanda climática", None), ("klimaklage", None),
        ("环保处罚", None), ("環境訴訟", None),
    ]),
]


# --------------------------------------------------------------------------
# Where the story is.  This is the region the finding concerns, not the region
# the wire was read from — a Japanese outlet reporting on the Amazon files
# under Latin America.  A story with global scope files under Global, and one
# can carry several: a study spanning Africa and South Asia files under both.
# --------------------------------------------------------------------------
GEO = [
    ("africa", "Africa", [
        ("africa*", None), ("sahel", None), ("congo basin", None), ("nigeria*", None),
        ("kenya*", None), ("ethiopia*", None), ("democratic republic of congo", None),
        ("drc", None), ("ghana", None), ("tanzania*", None), ("uganda*", None),
        ("south africa*", None), ("zimbabwe*", None), ("zambia*", None), ("mozambique", None),
        ("angola*", None), ("senegal", None), ("mali", ["africa", "sahel", "bamako", "drought"]),
        ("chad", ["lake", "africa", "sahel", "basin"]), ("sudan*", None), ("somalia*", None),
        ("madagascar", None), ("cameroon", None), ("côte d'ivoire", None), ("ivory coast", None),
        ("botswana", None), ("namibia", None), ("malawi", None), ("rwanda", None),
        ("okavango", None), ("lake victoria", None), ("serengeti", None), ("kalahari", None),
        ("horn of africa", None), ("afrique", None), ("áfrica", None), ("afrika", None),
        ("非洲", None), ("アフリカ", None), ("африк*", None), ("أفريقيا", None), ("अफ्रीका", None),
    ]),
    ("mena", "Middle East & North Africa", [
        ("middle east*", None), ("egypt*", None), ("morocco", None), ("algeria*", None),
        ("tunisia*", None), ("libya*", None), ("saudi arabia", None), ("emirates", None),
        ("qatar", None), ("kuwait", None), ("oman", None), ("yemen*", None), ("iraq*", None),
        ("iran*", None), ("israel*", None), ("palestin*", None), ("gaza", None), ("jordan", None),
        ("lebanon", None), ("syria*", None), ("turkey", ["drought", "climate", "pollution", "earthquake", "istanbul", "anatolia"]),
        ("türkiye", None), ("persian gulf", None), ("red sea", None), ("euphrates", None),
        ("tigris", None), ("dead sea", None), ("sahara", None), ("الشرق الأوسط", None),
        ("中东", None), ("北アフリカ", None),
    ]),
    ("asia", "Asia", [
        ("asia*", None), ("china", None), ("chinese", ["government", "province", "coal", "emissions", "cities"]),
        ("japan*", None), ("korea*", None), ("india", None), ("indian", ["ocean", "government", "farmers", "cities", "monsoon", "state"]),
        ("pakistan*", None), ("bangladesh*", None), ("nepal*", None), ("sri lanka", None),
        ("indonesia*", None), ("vietnam*", None), ("thailand", None), ("philippines", None),
        ("malaysia*", None), ("myanmar", None), ("cambodia*", None), ("laos", None),
        ("mongolia*", None), ("kazakhstan", None), ("uzbekistan", None), ("central asia", None),
        ("himalaya*", None), ("mekong", None), ("ganges", None), ("yangtze", None),
        ("brahmaputra", None), ("tibet*", None), ("borneo", None), ("sumatra", None),
        ("aral sea", None), ("gobi", None), ("siberia*", None), ("アジア", None), ("亚洲", None),
        ("아시아", None), ("एशिया", None), ("азия", None),
    ]),
    ("europe", "Europe", [
        ("europe*", ["union", "countries", "climate", "commission", "continent", "wide", "study", "across"]),
        ("european union", None), ("european commission", None), ("brussels", None),
        ("eu", ["deforestation", "regulation", "law", "directive", "commission", "member states",
                "emissions", "green deal", "farm", "policy", "ban", "target"]),
        ("united kingdom", None), ("britain", None), ("england", None),
        ("scotland", None), ("wales", ["climate", "flood", "farm", "coast"]), ("ireland", None),
        ("france", None), ("germany", None), ("spain", None), ("portugal", None), ("italy", None),
        ("greece", None), ("netherlands", None), ("belgium", None), ("poland", None),
        ("ukraine", None), ("russia*", None), ("sweden", None), ("norway", None), ("finland", None),
        ("denmark", None), ("switzerland", None), ("austria", None), ("romania", None),
        ("hungary", None), ("czech*", None), ("balkans", None), ("danube", None), ("alps", None),
        ("mediterranean", None), ("baltic", None), ("北欧", None), ("欧洲", None), ("ヨーロッパ", None),
        ("유럽", None), ("европ*", None), ("أوروبا", None),
    ]),
    ("latam", "Latin America & Caribbean", [
        ("latin america*", None), ("south america*", None), ("central america*", None),
        ("brazil*", None), ("brasil", None), ("amazon", None), ("amazônia", None), ("amazonía", None),
        ("argentina", None), ("chile", None), ("peru", None), ("colombia*", None),
        ("venezuela*", None), ("ecuador", None), ("bolivia*", None), ("paraguay", None),
        ("uruguay", None), ("mexico", None), ("méxico", None), ("guatemala", None),
        ("honduras", None), ("nicaragua", None), ("costa rica", None), ("panama", None),
        ("cuba", None), ("haiti", None), ("dominican republic", None), ("caribbean", None),
        ("patagonia", None), ("andes", None), ("cerrado", None), ("pantanal", None),
        ("gran chaco", None), ("orinoco", None), ("américa latina", None), ("拉丁美洲", None),
        ("ラテンアメリカ", None), ("латинская америка", None),
    ]),
    ("northam", "North America", [
        ("united states", None), ("u.s.", None), ("usa", None), ("american", ["government", "cities", "states", "west", "farmers", "midwest", "coast"]),
        ("canada", None), ("canadian", None), ("alaska*", None), ("california", None),
        ("texas", None), ("florida", None), ("great lakes", None), ("colorado river", None),
        ("mississippi", None), ("appalachia*", None), ("quebec", None), ("ontario", None),
        ("british columbia", None), ("gulf of mexico", None), ("états-unis", None),
        ("estados unidos", None), ("美国", None), ("加拿大", None), ("アメリカ合衆国", None),
        ("미국", None), ("сша", None),
    ]),
    ("oceania", "Oceania", [
        ("australia*", None), ("new zealand", None), ("aotearoa", None), ("papua", None),
        ("pacific island*", None), ("fiji", None), ("samoa", None), ("tonga", None),
        ("vanuatu", None), ("solomon islands", None), ("kiribati", None), ("tuvalu", None),
        ("great barrier reef", None), ("tasmania*", None), ("murray-darling", None),
        ("オセアニア", None), ("大洋洲", None), ("océanie", None),
    ]),
    ("polar", "Arctic & Antarctic", [
        ("arctic", None), ("antarctic*", None), ("greenland", None), ("svalbard", None),
        ("north pole", None), ("south pole", None), ("tundra", None), ("北極", None),
        ("南極", None), ("арктик*", None), ("antártic*", None), ("arctique", None),
    ]),
    ("ocean", "Oceans & high seas", [
        ("pacific ocean", None), ("atlantic ocean", None), ("indian ocean", None),
        ("southern ocean", None), ("high seas", None), ("open ocean", None),
        ("coral triangle", None), ("mariana", None), ("deep sea", None), ("north sea", None),
        ("bering sea", None), ("south china sea", None), ("océan pacifique", None),
        ("公海", None), ("深海", None),
    ]),
]

# --------------------------------------------------------------------------
# The gate.
#
# ANCHOR — the story is about environmental damage at all.
# SCOPE_GLOBAL / SCOPE_REGION — how wide the finding reaches.
# SYSTEMIC — mechanism words: tipping points, collapse, cascade, irreversible.
# CONSEQUENCE — what it does to people.
# MAGNITUDE — a number big enough to matter, matched by pattern, not by list.
#
# FINDING — a study, assessment or dataset rather than an incident.
#
# score = 2·global + 1·regional + 2·systemic + 1·magnitude + 1·consequence
#         + 1·finding.
# Below KEEP_SCORE the story never enters the feed. That is the whole point of
# this wire: the local, the incremental and the merely charming are dropped at
# the door.
# --------------------------------------------------------------------------
ANCHOR = [
    "environment*", "ecosystem*", "ecological", "climate", "warming", "emission*", "carbon",
    "greenhouse", "pollut*", "contaminat*", "toxic*", "deforest*", "forest loss", "logging",
    "biodiversity", "extinction", "species", "wildlife", "habitat", "ocean", "marine", "reef",
    "fisher*", "overfishing", "freshwater", "groundwater", "aquifer*", "drought", "flood*",
    "water scarcity", "water crisis", "depletion",
    "wildfire*", "heatwave*", "glacier*", "permafrost", "ice sheet", "sea level", "soil",
    "insect*", "insecte*", "insecto*", "昆虫", "pollinator*", "coral*", "reef*", "amphibian*",
    "seabird*", "fish population*", "food web*", "carbon sink*", "水资源", "水危机",
    "desertification", "wetland*", "peatland*", "mangrove*", "mining", "drilling", "fracking",
    "oil spill", "pesticide*", "microplastic*", "plastic*", "pfas", "mercury", "smog",
    "fossil fuel*", "coal", "waste", "sewage", "nitrogen", "eutrophication", "acidification",
    "environnement", "climat", "pollution", "biodiversité", "déforestation", "sécheresse",
    "medio ambiente", "clima", "contaminación", "biodiversidad", "deforestación", "sequía",
    "meio ambiente", "poluição", "desmatamento", "seca", "umwelt", "klima", "verschmutzung",
    "artensterben", "entwaldung", "dürre", "ambiente", "inquinamento", "siccità",
    "milieu", "klimaat", "vervuiling", "klimat", "zanieczyszczenie", "susza",
    "экология", "климат", "загрязнение", "засуха", "биоразнообраз", "вырубк",
    "довкілля", "環境", "気候", "汚染", "生物多様性", "干ばつ", "环境", "气候", "污染",
    "生态", "生物多样性", "干旱", "環保", "生態", "환경", "기후", "오염", "생태",
    "가뭄", "البيئة", "المناخ", "التلوث", "الجفاف", "التنوع البيولوجي", "पर्यावरण",
    "जलवायु", "प्रदूषण", "सूखा", "পরিবেশ", "জলবায়ু", "দূষণ", "lingkungan", "iklim",
    "pencemaran", "kekeringan", "çevre", "iklim", "kirlilik", "kuraklık", "môi trường",
    "khí hậu", "ô nhiễm", "สิ่งแวดล้อม", "ภูมิอากาศ", "มลพิษ", "mazingira", "tabianchi",
    "uchafuzi", "περιβάλλον", "κλίμα", "ρύπανση", "סביבה", "אקלים", "זיהום",
    "محیط زیست", "اقلیم", "آلودگی",
]

SCOPE_GLOBAL = [
    "global*", "worldwide", "world's", "planet*", "earth's", "across the world",
    "around the world", "international study", "every continent", "all continents",
    "humanity", "human civilisation", "human civilization", "the tropics", "the world over",
    "mondial*", "mundial*", "weltweit", "globale", "global", "по всему миру", "глобальн",
    "全球", "世界", "全世界", "世界的", "전 세계", "세계", "عالمي", "वैश्विक", "दुनिया",
    "বিশ্বব্যাপী", "global", "dunia", "küresel", "toàn cầu", "ทั่วโลก", "duniani",
    "παγκόσμι", "עולמי", "جهانی", "wereldwijd", "światow", "全球性",
]

SCOPE_REGION = [
    "amazon", "congo basin", "arctic", "antarctic", "himalaya*", "andes", "sahel", "sahara",
    "mediterranean", "great barrier reef", "coral triangle", "boreal", "siberia", "greenland",
    "pacific", "atlantic", "indian ocean", "southern ocean", "caribbean", "mekong", "ganges",
    "nile", "danube", "murray-darling", "colorado river", "horn of africa", "central asia",
    "southeast asia", "west africa", "east africa", "latin america", "south america",
    "north america", "europe", "asia", "africa", "oceania", "continent*", "hemisphere",
    "nationwide", "across the country", "across europe", "across asia", "across africa",
    "basin-wide", "region-wide",
]

SYSTEMIC = [
    "tipping point*", "tipping element*", "planetary boundar*", "collapse", "cascade",
    "feedback loop", "irreversible", "threshold", "regime shift", "mass extinction",
    "unprecedented", "record high", "record low", "record-breaking", "first time on record",
    "state of the climate", "global assessment", "meta-analysis", "systematic review",
    "ipcc", "ipbes", "unep", "wmo", "living planet report", "global forest watch",
    "red list", "point de bascule", "punto de inflexión", "punto de no retorno",
    "kipppunkt", "punto di non ritorno", "точка невозврата", "临界点", "転換点",
    "임계점", "نقطة التحول", "irreversibel", "onomkeerbaar", "nieodwracaln",
    "effondrement", "colapso", "kollaps", "коллапс", "崩壊", "崩溃", "붕괴",
]

CONSEQUENCE = [
    "deaths", "death toll", "mortality", "premature death*", "excess death*", "illness",
    "disease", "cancer", "malnutrition", "famine", "hunger", "food security", "water security",
    "displaced", "displacement", "refugee*", "migration", "livelihood*", "crop failure*",
    "yield*", "economic loss*", "gdp", "cost of", "billions of people", "millions of people",
    "children", "communities", "public health", "drinking water",
    "million people", "billion people", "people worldwide", "food supply", "food supplies",
    "water supply", "water supplies", "harvests", "livelihoods",
    "morts", "décès", "mortalité", "muertes", "mortes", "todesfälle", "смерт", "死亡",
    "사망", "وفيات", "मौतें", "মৃত্যু", "kematian", "ölüm", "tử vong", "เสียชีวิต",
    "vifo", "θάνατοι", "מוות", "مرگ",
]

MAGNITUDE = [
    re.compile(r"\b\d[\d.,]*\s?(%|percent|per cent|percento|pour cent|por ciento|prozent)", re.I),
    re.compile(r"\b\d[\d.,]*\s?(billion|million|trillion|thousand|bn|tn)\b", re.I),
    re.compile(r"\b\d[\d.,]*\s?(hectares?|acres?|square kilometres?|square kilometers?|km2|km²|sq km)\b", re.I),
    re.compile(r"\b\d[\d.,]*\s?(gigatonnes?|gigatons?|megatonnes?|tonnes?|tons?)\b", re.I),
    re.compile(r"\b\d[\d.,]*\s?(species|countries|nations|sites|rivers|cities|studies)\b", re.I),
    re.compile(r"(millones|milhões|milliards?|millionen|miliardi|миллион|миллиард|亿|万亿|百万|億|万|억|만|مليون|मिलियन|करोड़|juta)", re.I),
    # magnitudes written as words rather than digits — "two billion people"
    re.compile(r"\b(million|billion|trillion)s?\b", re.I),
    re.compile(r"\b(hundreds|thousands|tens|dozens)\s+of\s+(thousands|millions|species|sites|rivers|communities|hectares)\b", re.I),
]

# A generalising finding — a study, an assessment, a dataset — is itself a sign
# that the story is about a pattern rather than an incident.
FINDING = [
    "study finds", "study shows", "new study", "study published", "research finds",
    "researchers found", "scientists found", "scientists say", "report finds", "report warns",
    "analysis of", "assessment finds", "data shows", "dataset", "peer-reviewed",
    "published in nature", "published in science", "modelling shows", "survey of",
    "étude", "estudio", "estudo", "studie", "studio", "onderzoek", "badanie",
    "исследование", "дослідження", "研究", "調査", "연구", "دراسة", "अध्ययन", "গবেষণা",
    "penelitian", "araştırma", "nghiên cứu", "การศึกษา", "utafiti", "μελέτη", "מחקר", "مطالعه",
]

BLOCK = [
    # lifestyle, commerce and horoscopes wearing green clothing
    "gift guide", "best deals", "prime day", "black friday", "shopping guide", "coupon",
    "recipe", "restaurant review", "fashion week", "sustainable fashion collection",
    "horoscope", "astrolog*", "zodiac", "celebrity", "red carpet", "box office",
    "sports", "football", "premier league", "nba", "olympic medal",
    # weather-as-weather rather than climate
    "weather forecast", "forecast for the weekend", "five-day forecast", "tomorrow's weather",
    # promotional
    "webinar registration", "press release distribution", "sponsored content", "advertorial",
]


ANCHOR_C = _compile_all(ANCHOR)
GLOBAL_C = _compile_all(SCOPE_GLOBAL)
REGION_C = _compile_all(SCOPE_REGION)
SYSTEMIC_C = _compile_all(SYSTEMIC)
CONSEQUENCE_C = _compile_all(CONSEQUENCE)
FINDING_C = _compile_all(FINDING)
BLOCK_C = _compile_all(BLOCK)
TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]
GEO_C = [(gid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
         for gid, label, terms in GEO]


def magnitude(text):
    return any(rx.search(text) for rx in MAGNITUDE)


def score(text):
    """How big a picture is this? Returns (score, reasons)."""
    reasons = []
    total = 0
    if hit(text, GLOBAL_C):
        total += 2
        reasons.append("global")
    elif hit(text, REGION_C):
        total += 1
        reasons.append("regional")
    if hit(text, SYSTEMIC_C):
        total += 2
        reasons.append("systemic")
    if magnitude(text):
        total += 1
        reasons.append("magnitude")
    if hit(text, CONSEQUENCE_C):
        total += 1
        reasons.append("consequence")
    if hit(text, FINDING_C):
        total += 1
        reasons.append("finding")
    return total, reasons


def regions_for(text, global_scope):
    """Which parts of the world the finding concerns. Global scope counts as a
    region of its own, so a planetary study is findable without guessing where."""
    hits = ["global"] if global_scope else []
    for gid, _label, terms in GEO_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(gid)
            break
    return hits or ["unlocated"]


def topics_for(text):
    hits = []
    for tid, _label, terms in TOPICS_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(tid)
            break
    return hits


def load_sources():
    with open(SOURCES_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)
    srcs = []
    for s in cfg.get("direct", []):
        srcs.append({"name": s["name"], "lang": s["lang"], "region": s["region"],
                     "kind": s.get("kind", "news"), "url": s["url"]})
    for block, prefix, kind in (("gnews", "Google News · ", "news"),
                                ("scope", "Planet scale · ", "scope")):
        for loc in cfg.get(block, []):
            srcs.append({"name": prefix + loc["label"], "lang": loc["lang"],
                         "region": loc["region"], "kind": kind,
                         "url": build_gnews_url(loc)})
    return srcs, cfg


def run(dry_run=False, fixtures=None):
    sources, cfg = load_sources()
    print("Reading %d wires…" % len(sources))

    def read(src):
        if fixtures:
            path = os.path.join(fixtures, re.sub(r"[^\w.-]", "_", src["name"]) + ".xml")
            if not os.path.exists(path):
                return src, None
            with open(path, "rb") as fh:
                return src, fh.read()
        return src, fetch(src["url"])

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for src, raw in pool.map(read, sources):
            results.append((src, raw))

    previous = []
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH, encoding="utf-8") as fh:
                previous = json.load(fh).get("items", [])
        except Exception:  # noqa: BLE001
            previous = []

    seen_fp, seen_url, items = set(), set(), []

    def absorb(row):
        fp = fingerprint(row["t"])
        cu = canon_url(row["u"])
        if fp in seen_fp or cu in seen_url:
            return False
        seen_fp.add(fp)
        seen_url.add(cu)
        items.append(row)
        return True

    stats, ok_count, seen_but_small = [], 0, 0
    for src, raw in results:
        stat = {"name": src["name"], "lang": src["lang"], "region": src["region"],
                "kept": 0, "small": 0, "ok": False}
        if raw:
            stat["ok"] = True
            ok_count += 1
            for row in parse_feed(raw, src):
                text = (row["t"] + " " + row["s"]).lower()
                if hit(text, BLOCK_C) or not hit(text, ANCHOR_C):
                    continue
                total, reasons = score(text)
                if total < KEEP_SCORE:
                    stat["small"] += 1
                    seen_but_small += 1
                    continue
                row["x"] = topics_for(text) or ["climate"]
                row["p"] = total
                row["y"] = reasons
                row["w"] = regions_for(text, "global" in reasons)
                if absorb(row):
                    stat["kept"] += 1
        stats.append(stat)
        print("  %-34s %s" % (src["name"][:34],
                              "unreachable" if not raw
                              else "%d kept, %d too small" % (stat["kept"], stat["small"])))

    fresh_urls = {canon_url(i["u"]) for i in items}
    for row in previous:
        if "x" in row:
            absorb(row)

    cutoff = int(time.time() * 1000) - RETAIN_DAYS * 86400000
    items = [i for i in items if (i.get("d") or cutoff + 1) >= cutoff]
    items.sort(key=lambda i: i.get("d") or 0, reverse=True)
    items = items[:MAX_ITEMS]
    fresh = sum(1 for i in items if canon_url(i["u"]) in fresh_urls)

    languages = {}
    for loc in cfg.get("gnews", []):
        languages.setdefault(loc["lang"], re.sub(r"\s*\(.*\)$", "", loc["label"]))
    languages.setdefault("en", "English")

    regions = []
    for s in stats:
        if s["region"] not in regions:
            regions.append(s["region"])

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counts": {"stories": len(items), "new_this_run": fresh,
                   "languages": len({i["g"] for i in items}),
                   "big_picture": sum(1 for i in items if i.get("p", 0) >= BIG_PICTURE_SCORE),
                   "dropped_as_small": seen_but_small,
                   "wires_ok": ok_count, "wires_total": len(sources)},
        "big_picture_score": BIG_PICTURE_SCORE,
        "languages": languages,
        "regions": regions,
        "topics": [{"id": tid, "label": label} for tid, label, _ in TOPICS],
        "geo": ([{"id": "global", "label": "Global"}] +
                [{"id": gid, "label": label} for gid, label, _ in GEO] +
                [{"id": "unlocated", "label": "Unplaced"}]),
        "sources": stats,
        "items": items,
    }

    print("\n%d stories (%d new, %d at big-picture scale) · %d dropped as too small · %d languages · %d/%d wires answered"
          % (len(items), fresh, payload["counts"]["big_picture"], seen_but_small,
             payload["counts"]["languages"], ok_count, len(sources)))

    if dry_run:
        print("\n--dry-run: wire_env.json not written")
        return payload

    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    print("Wrote %s (%.0f KB)" % (OUT_PATH, os.path.getsize(OUT_PATH) / 1024))
    return payload


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fixtures")
    args = ap.parse_args()
    run(dry_run=args.dry_run, fixtures=args.fixtures)
