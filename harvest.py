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
mechanism, or a human consequence. A single-site population survey scores nothing
and never appears. "Freshwater species have declined 85% since 1970, threatening
food supplies for 200 million people" scores on all four.

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
WORKERS = 10         # a few hundred wires now
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
        # substring matching is already prefix-like in scripts without word
        # breaks, so a trailing * is a no-op — strip it rather than search for
        # a literal asterisk, which is what used to happen.
        return term[:-1] if term.endswith("*") else term
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
# Where the story is, in three levels: region, subregion, place. A story
# naming a place files under the subregion and region above it, so the page
# can open a region and drill into it.
# --------------------------------------------------------------------------
# region → subregion → country, with the terms that match each country.
# Matching a country implies its subregion and its region, so a story naming
# Peru files under Peru, South America and Latin America at once.
GEO3 = [
 ("africa", "Africa", [
   ("africa-e", "East Africa", [
     ("ke","Kenya",["kenya","kenyan","nairobi","ogiek","maasai","samburu","turkana"]),
     ("tz","Tanzania",["tanzania","tanzanian","ngorongoro","hadza","serengeti"]),
     ("ug","Uganda",["uganda","ugandan","batwa uganda","karamoja"]),
     ("et","Ethiopia",["ethiopia","ethiopian","omo valley","oromia"]),
     ("so","Somalia",["somalia","somali","somaliland"]),
     ("rw","Rwanda",["rwanda","rwandan"]),
     ("bi","Burundi",["burundi"]),
     ("sd","Sudan",["sudan","sudanese","darfur"]),
     ("ss","South Sudan",["south sudan","dinka","nuer"]),
     ("mg","Madagascar",["madagascar","malagasy"]),
     ("mz","Mozambique",["mozambique","cabo delgado"]),
     ("zm","Zambia",["zambia","zambian"]),
     ("zw","Zimbabwe",["zimbabwe","zimbabwean"]),
     ("mw","Malawi",["malawi"]),
   ]),
   ("africa-w", "West Africa", [
     ("ng","Nigeria",["nigeria","nigerian","ogoni","niger delta","ijaw"]),
     ("gh","Ghana",["ghana","ghanaian"]),
     ("ci","Côte d'Ivoire",["côte d'ivoire","ivory coast","ivorian"]),
     ("sn","Senegal",["senegal","senegalese","casamance"]),
     ("ml","Mali",["mali","malian","bamako","tuareg"]),
     ("bf","Burkina Faso",["burkina faso"]),
     ("ne","Niger",["niger republic","nigerien"]),
     ("lr","Liberia",["liberia","liberian"]),
     ("sl","Sierra Leone",["sierra leone"]),
     ("gn","Guinea",["guinea conakry","guinean"]),
     ("cm","Cameroon",["cameroon","cameroonian","baka"]),
   ]),
   ("africa-c", "Central Africa", [
     ("cd","DR Congo",["democratic republic of congo","drc","congolese","kivu","batwa"]),
     ("cg","Congo-Brazzaville",["republic of congo","brazzaville"]),
     ("ga","Gabon",["gabon","gabonese"]),
     ("cf","Central African Republic",["central african republic"]),
     ("td","Chad",["chad","chadian"]),
   ]),
   ("africa-s", "Southern Africa", [
     ("za","South Africa",["south africa","south african","khoisan","khoi","xolobeni"]),
     ("bw","Botswana",["botswana","san people","central kalahari"]),
     ("na","Namibia",["namibia","namibian","himba","ovahimba"]),
     ("ao","Angola",["angola","angolan"]),
     ("ls","Lesotho",["lesotho"]),
   ]),
   ("africa-n", "North Africa", [
     ("ma","Morocco",["morocco","moroccan","amazigh","berber","western sahara","sahrawi"]),
     ("dz","Algeria",["algeria","algerian","kabyle"]),
     ("tn","Tunisia",["tunisia"]),
     ("ly","Libya",["libya","libyan","tuareg libya"]),
     ("eg","Egypt",["egypt","egyptian","nubian"]),
   ]),
 ]),
 ("americas-n", "North America", [
   ("na-us", "United States", [
     ("us-ak","Alaska",["alaska","alaskan","inupiat","yupik","gwich'in"]),
     ("us-sw","US Southwest",["navajo","diné","hopi","apache","arizona tribe","new mexico pueblo","tohono o'odham"]),
     ("us-pl","US Plains & Midwest",["standing rock","lakota","dakota access","oglala","cheyenne river","ojibwe","anishinaabe"]),
     ("us-pnw","US Pacific Northwest",["yakama","nez perce","puyallup","lummi","columbia river treaty","klamath"]),
     ("us-e","US East & South",["cherokee","seminole","lumbee","penobscot","wampanoag","mashpee"]),
     ("us-hi","Hawai'i",["native hawaiian","kanaka maoli","mauna kea","hawaii"]),
   ]),
   ("na-ca", "Canada", [
     ("ca-bc","British Columbia",["british columbia","wet'suwet'en","haida","coastal gitxsan","secwepemc"]),
     ("ca-pr","Prairies",["alberta","saskatchewan","manitoba","treaty 8","treaty 6"]),
     ("ca-on","Ontario & Quebec",["ontario first nation","quebec","grassy narrows","innu","cree quebec","atikamekw"]),
     ("ca-n","Northern Canada",["nunavut","northwest territories","yukon","inuit nunangat","dene"]),
     ("ca-at","Atlantic Canada",["mi'kmaq","nova scotia","new brunswick","newfoundland","innu labrador"]),
   ]),
   ("na-mx", "Mexico", [
     ("mx-s","Southern Mexico",["chiapas","oaxaca","zapatista","zapoteco","mixe","tren maya","yucatán","maya"]),
     ("mx-n","Northern Mexico",["yaqui","rarámuri","tarahumara","sonora","chihuahua"]),
   ]),
 ]),
 ("americas-s", "Latin America & Caribbean", [
   ("la-amz", "Amazon Basin", [
     ("br-amz","Brazilian Amazon",["yanomami","munduruku","kayapó","xingu","terra indígena","amazônia","rondônia","pará"]),
     ("pe-amz","Peruvian Amazon",["loreto","ucayali","madre de dios","awajún","shipibo","kakataibo"]),
     ("co-amz","Colombian Amazon",["amazonas colombia","putumayo","caquetá"]),
     ("ec-amz","Ecuadorian Amazon",["yasuní","waorani","sarayaku","sucumbíos","achuar"]),
     ("bo-amz","Bolivian Amazon",["tipnis","beni","chiquitano","bolivian amazon"]),
     ("ve-amz","Venezuelan Amazon",["arco minero","amazonas venezuela","pemón"]),
   ]),
   ("la-and", "Andes & Southern Cone", [
     ("cl","Chile",["chile","chilean","mapuche","araucanía","wallmapu"]),
     ("ar","Argentina",["argentina","argentine","patagonia","qom","wichí"]),
     ("pe","Peru",["peru","peruvian","quechua","aymara peru"]),
     ("bo","Bolivia",["bolivia","bolivian","aymara","quechua bolivia"]),
     ("py","Paraguay",["paraguay","ayoreo","chaco paraguayo"]),
     ("uy","Uruguay",["uruguay"]),
   ]),
   ("la-ca", "Central America", [
     ("gt","Guatemala",["guatemala","guatemalan","ixil","k'iche'","q'eqchi'"]),
     ("hn","Honduras",["honduras","garífuna","lenca","berta cáceres"]),
     ("ni","Nicaragua",["nicaragua","miskito","bosawás"]),
     ("cr","Costa Rica",["costa rica","bribri","térraba"]),
     ("pa","Panama",["panama","guna","ngäbe","emberá"]),
     ("bz","Belize",["belize","maya belize"]),
     ("sv","El Salvador",["el salvador"]),
   ]),
   ("la-car", "Caribbean & Guianas", [
     ("gy","Guyana",["guyana","wapichan","rupununi"]),
     ("sr","Suriname",["suriname","saamaka","maroon suriname","kaliña"]),
     ("gf","French Guiana",["guyane","french guiana","wayana"]),
     ("do","Caribbean islands",["dominica kalinago","caribbean indigenous","taino","haiti","jamaica","puerto rico"]),
   ]),
   ("la-br", "Brazil (other)", [
     ("br-ne","Brazil northeast & cerrado",["cerrado","bahia indígena","maranhão","quilombola","pataxó","guarani-kaiowá","mato grosso do sul"]),
   ]),
 ]),
 ("asia-s", "South Asia", [
   ("sa-in", "India", [
     ("in-c","Central India",["chhattisgarh","jharkhand","odisha","madhya pradesh","hasdeo","niyamgiri","bastar"]),
     ("in-ne","Northeast India",["assam","manipur","nagaland","mizoram","meghalaya","arunachal"]),
     ("in-s","South & West India",["kerala adivasi","tamil nadu tribal","karnataka tribal","gujarat adivasi","maharashtra adivasi"]),
     ("in-h","Himalayan India",["ladakh","uttarakhand","himachal","sikkim"]),
   ]),
   ("sa-oth", "Rest of South Asia", [
     ("bd","Bangladesh",["bangladesh","chittagong hill tracts","jumma","chakma"]),
     ("np","Nepal",["nepal","tharu","newar","chepang"]),
     ("pk","Pakistan",["pakistan","balochistan","kalash"]),
     ("lk","Sri Lanka",["sri lanka","vedda"]),
     ("bt","Bhutan",["bhutan"]),
   ]),
 ]),
 ("asia-se", "Southeast Asia", [
   ("se-mar", "Maritime Southeast Asia", [
     ("id","Indonesia",["indonesia","indonesian","masyarakat adat","papua","west papua","kalimantan","dayak","sulawesi","sumatra","mentawai"]),
     ("ph","Philippines",["philippines","filipino","lumad","igorot","mindanao","cordillera","ancestral domain"]),
     ("my","Malaysia",["malaysia","sarawak","sabah","penan","orang asli","bakun"]),
     ("tl","Timor-Leste",["timor-leste","east timor"]),
     ("pg-ind","Papua New Guinea",["papua new guinea","bougainville","porgera"]),
   ]),
   ("se-main", "Mainland Southeast Asia", [
     ("th","Thailand",["thailand","karen thailand","bangkloi","chao lay","hill tribe"]),
     ("mm","Myanmar",["myanmar","burma","karen state","kachin","chin state","rakhine"]),
     ("vn","Vietnam",["vietnam","montagnard","central highlands vietnam"]),
     ("kh","Cambodia",["cambodia","bunong","ratanakiri"]),
     ("la","Laos",["laos","hmong laos"]),
   ]),
 ]),
 ("asia-e", "East & Central Asia", [
   ("ea-e", "East Asia", [
     ("tw","Taiwan",["taiwan","原住民族","傳統領域","amis","atayal","bunun"]),
     ("jp","Japan",["japan","ainu","hokkaido","okinawa","ryukyu"]),
     ("cn","China",["china","tibet","tibetan","xinjiang","uyghur","inner mongolia","yunnan minority"]),
     ("kr","Korea",["korea","korean"]),
     ("mn","Mongolia",["mongolia","mongolian","dukha","tsaatan"]),
   ]),
   ("ea-c", "Central Asia & Siberia", [
     ("ru-sib","Siberia & Russian North",["siberia","evenki","nenets","khanty","yamal","sakha","chukotka","коренные малочисленные"]),
     ("kz","Kazakhstan",["kazakhstan"]),
     ("kg","Kyrgyzstan",["kyrgyzstan"]),
     ("uz","Uzbekistan",["uzbekistan"]),
   ]),
 ]),
 ("mena", "Middle East & North Africa", [
   ("me-lev", "Levant & Gulf", [
     ("il","Israel & Palestine",["bedouin","negev","naqab","palestinian land","israel","west bank"]),
     ("jo","Jordan",["jordan","bedouin jordan"]),
     ("iq","Iraq",["iraq","marsh arabs","yazidi","kurdistan iraq"]),
     ("ir","Iran",["iran","qashqai","bakhtiari","ahwazi"]),
     ("sa","Gulf states",["saudi arabia","uae","oman","qatar","kuwait"]),
     ("tr","Turkey",["turkey","türkiye","kurdish","hasankeyf","alevi"]),
   ]),
 ]),
 ("europe", "Europe", [
   ("eu-n", "Nordic & Arctic Europe", [
     ("no","Norway",["norway","norwegian","sápmi","fosen","finnmark"]),
     ("se","Sweden",["sweden","swedish","girjas","gällivare","kiruna","samer"]),
     ("fi","Finland",["finland","finnish","inari","sámi parliament"]),
     ("gl","Greenland",["greenland","kalaallit","nuuk"]),
     ("ru-eu","Russian Karelia & Kola",["kola peninsula","karelia","murmansk sami"]),
   ]),
   ("eu-o", "Rest of Europe", [
     ("ua","Ukraine",["ukraine","crimean tatars","krym"]),
     ("ru","Russia (European)",["russia","russian federation"]),
     ("eu","European Union",["european union","european commission","brussels"]),
     ("uk","United Kingdom",["united kingdom","britain","scotland","wales"]),
     ("es","Spain",["spain","spanish"]),
     ("fr","France",["france","french"]),
     ("de","Germany",["germany","german"]),
   ]),
 ]),
 ("oceania", "Oceania", [
   ("oc-au", "Australia", [
     ("au-n","Northern Australia",["northern territory","arnhem land","kimberley","juukan gorge","tiwi","gulf country"]),
     ("au-w","Western Australia",["western australia","pilbara","noongar","yindjibarndi"]),
     ("au-e","Eastern Australia",["queensland","new south wales","victoria aboriginal","wiradjuri","gunditjmara","adani","carmichael"]),
     ("au-c","Central & South Australia",["south australia","adnyamathanha","arrernte","alice springs","olympic dam"]),
   ]),
   ("oc-nz", "Aotearoa New Zealand", [
     ("nz","Aotearoa",["new zealand","aotearoa","māori","maori","iwi","waitangi","ngāi tahu","tainui"]),
   ]),
   ("oc-pac", "Pacific Islands", [
     ("fj","Fiji",["fiji","fijian","itaukei"]),
     ("nc","Kanaky New Caledonia",["new caledonia","kanaky","kanak","nouméa"]),
     ("sb","Solomon Islands",["solomon islands"]),
     ("vu","Vanuatu",["vanuatu","ni-vanuatu"]),
     ("ws","Polynesia & Micronesia",["samoa","tonga","tuvalu","kiribati","marshall islands","palau","guam","chamorro","tahiti","rapa nui","easter island"]),
   ]),
 ]),
 ("polar", "Arctic & Antarctic", [
   ("pol-arc", "Circumpolar", [
     ("arctic","Arctic Council region",["arctic council","circumpolar","inuit circumpolar","arctic indigenous"]),
   ]),
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
GEO3_C = [(rid, rlabel, [(sid, slabel, [(pid, plabel, _compile_all(terms))
                                        for pid, plabel, terms in places])
                        for sid, slabel, places in subs])
          for rid, rlabel, subs in GEO3]


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


def places_for(text):
    """Returns (regions, subregions, places). Naming a place implies the
    subregion and region above it. A worldwide finding that names nowhere in
    particular stays unlocated — its reach is already in the scale reasons."""
    regions, subs, places = [], [], []
    for rid, _rl, sublist in GEO3_C:
        for sid, _sl, plist in sublist:
            for pid, _pl, terms in plist:
                if not hit(text, terms):
                    continue
                if pid not in places:
                    places.append(pid)
                if sid not in subs:
                    subs.append(sid)
                if rid not in regions:
                    regions.append(rid)
    return (regions or ["unlocated"], subs or ["unlocated"], places or ["unlocated"])


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


COORDS = {
 # --- regions ---
 "africa": [1.5, 20.0], "americas-n": [45.0, -100.0], "americas-s": [-12.0, -60.0],
 "asia-s": [22.0, 79.0], "asia-se": [2.0, 112.0], "asia-e": [40.0, 100.0],
 "mena": [28.0, 42.0], "europe": [52.0, 15.0], "oceania": [-25.0, 140.0], "polar": [78.0, 0.0],
 # --- subregions ---
 "africa-e": [1.0, 37.0], "africa-w": [10.0, -2.0], "africa-c": [0.0, 20.0],
 "africa-s": [-24.0, 24.0], "africa-n": [28.0, 12.0],
 "na-us": [39.0, -98.0], "na-ca": [58.0, -100.0], "na-mx": [23.0, -102.0],
 "la-amz": [-4.0, -62.0], "la-and": [-25.0, -68.0], "la-ca": [14.0, -87.0],
 "la-car": [8.0, -60.0], "la-br": [-13.0, -47.0],
 "sa-in": [22.0, 79.0], "sa-oth": [27.0, 85.0],
 "se-mar": [-2.0, 118.0], "se-main": [16.0, 101.0],
 "ea-e": [35.0, 118.0], "ea-c": [50.0, 80.0],
 "me-lev": [32.0, 40.0],
 "eu-n": [65.0, 20.0], "eu-o": [50.0, 15.0],
 "oc-au": [-25.0, 134.0], "oc-nz": [-41.0, 174.0], "oc-pac": [-15.0, 170.0],
 "pol-arc": [80.0, 0.0],
 # --- places: Africa ---
 "ke": [0.2, 37.9], "tz": [-6.4, 34.9], "ug": [1.4, 32.3], "et": [9.1, 40.5],
 "so": [5.2, 46.2], "rw": [-1.9, 29.9], "bi": [-3.4, 29.9], "sd": [15.6, 30.2],
 "ss": [7.9, 30.0], "mg": [-18.8, 46.9], "mz": [-18.7, 35.5], "zm": [-13.1, 27.8],
 "zw": [-19.0, 29.2], "mw": [-13.3, 34.3],
 "ng": [9.1, 8.7], "gh": [7.9, -1.0], "ci": [7.5, -5.5], "sn": [14.5, -14.5],
 "ml": [17.6, -4.0], "bf": [12.2, -1.6], "ne": [17.6, 8.1], "lr": [6.4, -9.4],
 "sl": [8.5, -11.8], "gn": [9.9, -9.7], "cm": [7.4, 12.4], "sahel": [15.0, 2.0], "horn": [8.0, 45.0],
 "cd": [-4.0, 21.8], "cg": [-0.2, 15.8], "ga": [-0.8, 11.6], "cf": [6.6, 20.9], "td": [15.5, 18.7],
 "za": [-30.6, 22.9], "bw": [-22.3, 24.7], "na": [-22.9, 18.5], "ao": [-11.2, 17.9], "ls": [-29.6, 28.2],
 "ma": [31.8, -7.1], "dz": [28.0, 1.7], "tn": [33.9, 9.5], "ly": [26.3, 17.2], "eg": [26.8, 30.8],
 # --- places: North America ---
 "us-ak": [64.0, -152.0], "us-sw": [34.5, -110.0], "us-pl": [44.0, -100.0],
 "us-pnw": [46.5, -121.0], "us-e": [35.5, -80.0], "us-hi": [20.8, -156.3],
 "ca-bc": [54.0, -125.0], "ca-pr": [52.0, -106.0], "ca-on": [49.0, -80.0],
 "ca-n": [64.0, -105.0], "ca-at": [46.5, -63.0],
 "mx-s": [17.0, -94.0], "mx-n": [28.5, -108.0],
 # --- places: Latin America ---
 "br-amz": [-4.5, -60.0], "pe-amz": [-6.0, -75.0], "co-amz": [-1.0, -72.0],
 "ec-amz": [-1.5, -76.5], "bo-amz": [-14.5, -65.0], "ve-amz": [5.0, -65.0],
 "cl": [-35.7, -71.5], "ar": [-38.4, -63.6], "pe": [-9.2, -75.0], "bo": [-16.3, -63.6],
 "py": [-23.4, -58.4], "uy": [-32.5, -55.8],
 "gt": [15.8, -90.2], "hn": [15.2, -86.2], "ni": [12.9, -85.2], "cr": [9.7, -83.8],
 "pa": [8.5, -80.8], "bz": [17.2, -88.5], "sv": [13.8, -88.9],
 "gy": [4.9, -58.9], "sr": [3.9, -56.0], "gf": [3.9, -53.1], "do": [18.7, -70.2],
 "br-ne": [-10.0, -45.0],
 # --- places: South Asia ---
 "in-c": [21.5, 82.0], "in-ne": [26.0, 93.0], "in-s": [13.0, 77.5], "in-h": [32.0, 78.0],
 "bd": [23.7, 90.4], "np": [28.4, 84.1], "pk": [30.4, 69.3], "lk": [7.9, 80.8], "bt": [27.5, 90.4],
 # --- places: Southeast Asia ---
 "id": [-2.5, 118.0], "ph": [12.9, 121.8], "my": [4.2, 109.5], "tl": [-8.9, 125.7], "pg-ind": [-6.3, 143.9],
 "th": [15.9, 100.99], "mm": [21.9, 95.96], "vn": [14.1, 108.3], "kh": [12.6, 104.99], "la": [19.9, 102.5],
 # --- places: East & Central Asia ---
 "tw": [23.7, 121.0], "jp": [36.2, 138.3], "cn": [35.9, 104.2], "kr": [36.5, 127.9], "mn": [46.9, 103.8],
 "ru-sib": [62.0, 105.0], "kz": [48.0, 66.9], "kg": [41.2, 74.8], "uz": [41.4, 64.6],
 # --- places: MENA ---
 "il": [31.5, 35.0], "jo": [30.6, 36.2], "iq": [33.2, 43.7], "ir": [32.4, 53.7],
 "sa": [24.0, 45.0], "tr": [39.0, 35.2],
 # --- places: Europe ---
 "no": [64.6, 12.0], "se": [62.0, 15.0], "fi": [64.0, 26.0], "gl": [71.7, -42.6], "ru-eu": [67.5, 35.0],
 "ua": [48.4, 31.2], "ru": [56.0, 40.0], "eu": [50.8, 4.4], "uk": [54.0, -2.5],
 "es": [40.2, -3.7], "fr": [46.6, 2.4], "de": [51.2, 10.4],
 # --- places: Oceania ---
 "au-n": [-15.0, 133.0], "au-w": [-25.0, 121.0], "au-e": [-30.0, 148.0], "au-c": [-29.0, 135.0],
 "nz": [-41.0, 174.0], "fj": [-17.7, 178.0], "nc": [-21.3, 165.5], "sb": [-9.6, 160.2],
 "vu": [-15.4, 166.9], "ws": [-13.8, -172.1],
 # --- polar ---
 "arctic": [80.0, 0.0],
}

COORDS.update({
 "sy": [35.0, 38.0], "lb": [33.9, 35.9], "ye": [15.5, 48.0], "ps": [31.5, 34.5],
 "sy-kur": [36.4, 40.7], "cy": [35.1, 33.4],
 "af": [33.9, 67.7], "tj": [38.9, 71.3], "tm": [39.0, 59.6], "am": [40.1, 45.0],
 "az": [40.1, 47.6], "ge": [42.3, 43.4],
 "kp": [40.3, 127.5], "hk": [22.3, 114.2],
 "pl": [52.0, 19.1], "ro": [45.9, 25.0], "md": [47.0, 28.4], "by": [53.7, 27.95],
 "balt": [56.9, 24.6], "balk": [43.8, 20.5], "it": [42.5, 12.6], "nl": [52.1, 5.3],
 "at": [47.6, 12.0], "pt": [39.4, -8.2], "gr": [39.1, 22.0], "ie": [53.3, -8.0], "hu": [47.9, 18.5],
 "us": [39.0, -98.0], "ca": [58.0, -100.0], "mx": [23.0, -102.0], "br": [-13.0, -47.0],
 "co": [4.6, -74.1], "ve": [8.0, -66.0], "ht": [18.9, -72.3], "cu": [21.9, -79.5], "dom": [18.7, -70.2],
 "er": [15.3, 39.0], "dj": [11.8, 42.6], "km": [-11.9, 43.9],
 "bj": [8.5, 1.6], "mr": [20.0, -10.9], "gw": [12.4, -14.5],
 "au": [-25.0, 134.0], "bn": [4.5, 114.7], "mv": [3.2, 73.2],
})

# Sub-national placement. Matched before the country layer, so a story naming
# Kharkiv is pinned on Kharkiv rather than the middle of Ukraine. Longest match
# wins, so "north kivu" beats "kivu" and "port sudan" beats "sudan".
PRECISE = {
 # --- Ukraine & Russia ---
 "kyiv": ("Kyiv", 50.45, 30.52), "kiev": ("Kyiv", 50.45, 30.52),
 "kharkiv": ("Kharkiv", 49.99, 36.23), "odesa": ("Odesa", 46.48, 30.73),
 "odessa": ("Odesa", 46.48, 30.73), "lviv": ("Lviv", 49.84, 24.03),
 "dnipro": ("Dnipro", 48.46, 35.05), "zaporizhzhia": ("Zaporizhzhia", 47.84, 35.14),
 "kherson": ("Kherson", 46.64, 32.61), "mykolaiv": ("Mykolaiv", 46.98, 31.99),
 "donetsk": ("Donetsk", 48.02, 37.80), "luhansk": ("Luhansk", 48.57, 39.31),
 "donbas": ("Donbas", 48.30, 38.20), "mariupol": ("Mariupol", 47.10, 37.55),
 "bakhmut": ("Bakhmut", 48.60, 38.00), "avdiivka": ("Avdiivka", 48.14, 37.75),
 "pokrovsk": ("Pokrovsk", 48.28, 37.18), "kupiansk": ("Kupiansk", 49.71, 37.62),
 "sumy": ("Sumy", 50.91, 34.80), "chernihiv": ("Chernihiv", 51.49, 31.29),
 "crimea": ("Crimea", 45.30, 34.40), "sevastopol": ("Sevastopol", 44.62, 33.53),
 "moscow": ("Moscow", 55.75, 37.62), "kremlin": ("Moscow", 55.75, 37.62),
 "belgorod": ("Belgorod", 50.60, 36.59), "kursk region": ("Kursk", 51.73, 36.19),
 "rostov": ("Rostov-on-Don", 47.24, 39.71), "novorossiysk": ("Novorossiysk", 44.72, 37.77),
 "st petersburg": ("St Petersburg", 59.94, 30.31), "vladivostok": ("Vladivostok", 43.12, 131.89),
 # --- Israel, Palestine, Lebanon, Syria ---
 "gaza city": ("Gaza City", 31.51, 34.45), "gaza strip": ("Gaza", 31.42, 34.35),
 "gaza": ("Gaza", 31.42, 34.35), "rafah": ("Rafah", 31.29, 34.25),
 "khan younis": ("Khan Younis", 31.34, 34.30), "deir al-balah": ("Deir al-Balah", 31.42, 34.35),
 "west bank": ("West Bank", 31.95, 35.30), "jenin": ("Jenin", 32.46, 35.30),
 "nablus": ("Nablus", 32.22, 35.26), "hebron": ("Hebron", 31.53, 35.10),
 "ramallah": ("Ramallah", 31.90, 35.21), "jerusalem": ("Jerusalem", 31.78, 35.22),
 "tel aviv": ("Tel Aviv", 32.09, 34.78), "haifa": ("Haifa", 32.79, 34.99),
 "golan": ("Golan Heights", 32.95, 35.75), "sderot": ("Sderot", 31.52, 34.60),
 "beirut": ("Beirut", 33.89, 35.50), "south lebanon": ("South Lebanon", 33.30, 35.40),
 "tyre": ("Tyre", 33.27, 35.20), "baalbek": ("Baalbek", 34.01, 36.21),
 "damascus": ("Damascus", 33.51, 36.29), "aleppo": ("Aleppo", 36.20, 37.13),
 "idlib": ("Idlib", 35.93, 36.63), "homs": ("Homs", 34.73, 36.71),
 "latakia": ("Latakia", 35.52, 35.79), "deir ez-zor": ("Deir ez-Zor", 35.34, 40.14),
 "hasakah": ("Hasakah", 36.50, 40.75), "rojava": ("North-east Syria", 36.40, 40.70),
 # --- Iraq, Iran, Gulf, Yemen ---
 "baghdad": ("Baghdad", 33.31, 44.36), "mosul": ("Mosul", 36.35, 43.13),
 "erbil": ("Erbil", 36.19, 44.01), "basra": ("Basra", 30.51, 47.78),
 "fallujah": ("Fallujah", 33.35, 43.78), "kirkuk": ("Kirkuk", 35.47, 44.39),
 "tehran": ("Tehran", 35.69, 51.39), "isfahan": ("Isfahan", 32.65, 51.67),
 "natanz": ("Natanz", 33.72, 51.73), "fordow": ("Fordow", 34.88, 50.99),
 "bandar abbas": ("Bandar Abbas", 27.19, 56.28), "strait of hormuz": ("Strait of Hormuz", 26.57, 56.25),
 "riyadh": ("Riyadh", 24.71, 46.68), "jeddah": ("Jeddah", 21.49, 39.19),
 "doha": ("Doha", 25.29, 51.53), "al udeid": ("Al Udeid air base", 25.12, 51.32),
 "abu dhabi": ("Abu Dhabi", 24.45, 54.38), "dubai": ("Dubai", 25.20, 55.27),
 "manama": ("Manama", 26.23, 50.59), "kuwait city": ("Kuwait City", 29.38, 47.99),
 "muscat": ("Muscat", 23.59, 58.41),
 "sanaa": ("Sanaa", 15.37, 44.19), "sana'a": ("Sanaa", 15.37, 44.19),
 "aden": ("Aden", 12.79, 45.02), "hodeidah": ("Hodeidah", 14.80, 42.95),
 "marib": ("Marib", 15.46, 45.32), "bab el-mandeb": ("Bab el-Mandeb", 12.58, 43.33),
 "red sea": ("Red Sea", 20.00, 38.00),
 # --- Turkey, Caucasus, Central Asia, Afghanistan ---
 "ankara": ("Ankara", 39.93, 32.86), "istanbul": ("Istanbul", 41.01, 28.98),
 "incirlik": ("Incirlik air base", 37.00, 35.43), "diyarbakir": ("Diyarbakır", 37.91, 40.24),
 "yerevan": ("Yerevan", 40.18, 44.51), "baku": ("Baku", 40.41, 49.87),
 "nagorno-karabakh": ("Nagorno-Karabakh", 39.82, 46.75), "karabakh": ("Nagorno-Karabakh", 39.82, 46.75),
 "tbilisi": ("Tbilisi", 41.72, 44.78), "abkhazia": ("Abkhazia", 43.00, 41.00),
 "south ossetia": ("South Ossetia", 42.35, 43.97),
 "kabul": ("Kabul", 34.53, 69.17), "kandahar": ("Kandahar", 31.61, 65.71),
 "herat": ("Herat", 34.35, 62.20), "jalalabad": ("Jalalabad", 34.43, 70.45),
 "dushanbe": ("Dushanbe", 38.56, 68.79), "tashkent": ("Tashkent", 41.30, 69.24),
 "almaty": ("Almaty", 43.24, 76.89), "astana": ("Astana", 51.17, 71.45),
 # --- South Asia ---
 "islamabad": ("Islamabad", 33.68, 73.05), "rawalpindi": ("Rawalpindi", 33.60, 73.04),
 "karachi": ("Karachi", 24.86, 67.01), "peshawar": ("Peshawar", 34.01, 71.58),
 "quetta": ("Quetta", 30.18, 66.98), "balochistan": ("Balochistan", 28.50, 65.50),
 "kashmir": ("Kashmir", 34.08, 74.80), "srinagar": ("Srinagar", 34.08, 74.80),
 "line of control": ("Line of Control", 34.20, 74.20),
 "new delhi": ("New Delhi", 28.61, 77.21), "mumbai": ("Mumbai", 19.08, 72.88),
 "manipur": ("Manipur", 24.66, 93.91), "assam": ("Assam", 26.20, 92.94),
 "dhaka": ("Dhaka", 23.81, 90.41), "chittagong hill tracts": ("Chittagong Hill Tracts", 22.60, 92.20),
 "colombo": ("Colombo", 6.93, 79.86), "kathmandu": ("Kathmandu", 27.72, 85.32),
 # --- East & Southeast Asia ---
 "beijing": ("Beijing", 39.90, 116.41), "shanghai": ("Shanghai", 31.23, 121.47),
 "taiwan strait": ("Taiwan Strait", 24.50, 119.50), "taipei": ("Taipei", 25.03, 121.57),
 "kinmen": ("Kinmen", 24.44, 118.32), "south china sea": ("South China Sea", 13.00, 114.00),
 "spratly": ("Spratly Islands", 9.50, 114.00), "paracel": ("Paracel Islands", 16.50, 112.00),
 "scarborough shoal": ("Scarborough Shoal", 15.15, 117.76),
 "senkaku": ("Senkaku Islands", 25.75, 123.48), "diaoyu": ("Senkaku Islands", 25.75, 123.48),
 "xinjiang": ("Xinjiang", 41.00, 85.00), "tibet": ("Tibet", 31.00, 88.00),
 "pyongyang": ("Pyongyang", 39.04, 125.76), "yongbyon": ("Yongbyon", 39.80, 125.75),
 "panmunjom": ("Panmunjom", 37.96, 126.68), "seoul": ("Seoul", 37.57, 126.98),
 "tokyo": ("Tokyo", 35.68, 139.69), "okinawa": ("Okinawa", 26.34, 127.80),
 "guam": ("Guam", 13.44, 144.79), "manila": ("Manila", 14.60, 120.98),
 "mindanao": ("Mindanao", 7.50, 124.50), "jakarta": ("Jakarta", -6.21, 106.85),
 "west papua": ("West Papua", -4.00, 138.00), "papua": ("Papua", -4.00, 138.00),
 "naypyidaw": ("Naypyidaw", 19.75, 96.10), "yangon": ("Yangon", 16.87, 96.20),
 "rakhine": ("Rakhine State", 20.10, 93.50), "kachin": ("Kachin State", 25.80, 97.40),
 "karen state": ("Karen State", 17.30, 97.70), "shan state": ("Shan State", 21.50, 98.00),
 "bangkok": ("Bangkok", 13.76, 100.50), "hanoi": ("Hanoi", 21.03, 105.85),
 "phnom penh": ("Phnom Penh", 11.56, 104.92),
 # --- Africa ---
 "khartoum": ("Khartoum", 15.50, 32.56), "omdurman": ("Omdurman", 15.65, 32.48),
 "port sudan": ("Port Sudan", 19.62, 37.22), "darfur": ("Darfur", 13.00, 24.00),
 "el fasher": ("El Fasher", 13.63, 25.35), "nyala": ("Nyala", 12.05, 24.88),
 "juba": ("Juba", 4.85, 31.58), "addis ababa": ("Addis Ababa", 9.03, 38.74),
 "tigray": ("Tigray", 14.00, 38.50), "amhara": ("Amhara", 11.50, 38.00),
 "mekelle": ("Mekelle", 13.50, 39.47), "asmara": ("Asmara", 15.34, 38.93),
 "mogadishu": ("Mogadishu", 2.05, 45.32), "kismayo": ("Kismayo", -0.36, 42.55),
 "puntland": ("Puntland", 8.50, 49.00), "nairobi": ("Nairobi", -1.29, 36.82),
 "kinshasa": ("Kinshasa", -4.44, 15.27), "goma": ("Goma", -1.68, 29.22),
 "north kivu": ("North Kivu", -0.80, 29.00), "south kivu": ("South Kivu", -3.00, 28.30),
 "bukavu": ("Bukavu", -2.51, 28.86), "ituri": ("Ituri", 1.80, 29.90),
 "bangui": ("Bangui", 4.39, 18.56), "n'djamena": ("N'Djamena", 12.13, 15.06),
 "bamako": ("Bamako", 12.64, -8.00), "gao": ("Gao", 16.27, -0.04),
 "timbuktu": ("Timbuktu", 16.77, -3.01), "ouagadougou": ("Ouagadougou", 12.37, -1.52),
 "niamey": ("Niamey", 13.51, 2.13), "lake chad": ("Lake Chad basin", 13.00, 14.00),
 "abuja": ("Abuja", 9.06, 7.49), "borno": ("Borno State", 11.80, 13.10),
 "maiduguri": ("Maiduguri", 11.83, 13.15), "lagos": ("Lagos", 6.52, 3.38),
 "tripoli libya": ("Tripoli", 32.89, 13.19), "benghazi": ("Benghazi", 32.12, 20.07),
 "cairo": ("Cairo", 30.04, 31.24), "sinai": ("Sinai", 29.50, 33.80),
 "cabo delgado": ("Cabo Delgado", -12.50, 39.50), "maputo": ("Maputo", -25.97, 32.57),
 "harare": ("Harare", -17.83, 31.05), "pretoria": ("Pretoria", -25.75, 28.19),
 "johannesburg": ("Johannesburg", -26.20, 28.05), "cape town": ("Cape Town", -33.92, 18.42),
 # --- Europe ---
 "brussels": ("Brussels", 50.85, 4.35), "the hague": ("The Hague", 52.08, 4.31),
 "geneva": ("Geneva", 46.20, 6.14), "vienna": ("Vienna", 48.21, 16.37),
 "london": ("London", 51.51, -0.13), "paris": ("Paris", 48.86, 2.35),
 "berlin": ("Berlin", 52.52, 13.40), "ramstein": ("Ramstein air base", 49.44, 7.60),
 "rome": ("Rome", 41.90, 12.50), "madrid": ("Madrid", 40.42, -3.70),
 "warsaw": ("Warsaw", 52.23, 21.01), "rzeszow": ("Rzeszów", 50.04, 22.00),
 "kaliningrad": ("Kaliningrad", 54.71, 20.51), "suwalki": ("Suwałki gap", 54.10, 23.00),
 "minsk": ("Minsk", 53.90, 27.57), "chisinau": ("Chișinău", 47.01, 28.86),
 "transnistria": ("Transnistria", 47.20, 29.20), "vilnius": ("Vilnius", 54.69, 25.28),
 "riga": ("Riga", 56.95, 24.11), "tallinn": ("Tallinn", 59.44, 24.75),
 "helsinki": ("Helsinki", 60.17, 24.94), "stockholm": ("Stockholm", 59.33, 18.07),
 "oslo": ("Oslo", 59.91, 10.75), "gotland": ("Gotland", 57.50, 18.50),
 "belgrade": ("Belgrade", 44.79, 20.45), "pristina": ("Pristina", 42.66, 21.16),
 "sarajevo": ("Sarajevo", 43.86, 18.41), "black sea": ("Black Sea", 43.40, 34.30),
 "baltic sea": ("Baltic Sea", 57.00, 19.00), "arctic circle": ("Arctic", 70.00, 20.00),
 # --- Americas ---
 "washington": ("Washington DC", 38.91, -77.04), "pentagon": ("The Pentagon", 38.87, -77.06),
 "white house": ("White House", 38.90, -77.04), "new york": ("New York", 40.71, -74.01),
 "guantanamo": ("Guantánamo Bay", 19.90, -75.15), "diego garcia": ("Diego Garcia", -7.31, 72.41),
 "ottawa": ("Ottawa", 45.42, -75.70), "mexico city": ("Mexico City", 19.43, -99.13),
 "bogota": ("Bogotá", 4.71, -74.07), "bogotá": ("Bogotá", 4.71, -74.07),
 "caracas": ("Caracas", 10.49, -66.88), "essequibo": ("Essequibo", 6.00, -59.00),
 "port-au-prince": ("Port-au-Prince", 18.59, -72.31), "havana": ("Havana", 23.11, -82.37),
 "brasilia": ("Brasília", -15.79, -47.88), "brasília": ("Brasília", -15.79, -47.88),
 "buenos aires": ("Buenos Aires", -34.60, -58.38), "santiago": ("Santiago", -33.45, -70.67),
 "lima": ("Lima", -12.05, -77.04), "quito": ("Quito", -0.18, -78.47),
 "guayaquil": ("Guayaquil", -2.19, -79.89), "tegucigalpa": ("Tegucigalpa", 14.07, -87.19),
 "san salvador": ("San Salvador", 13.69, -89.19), "guatemala city": ("Guatemala City", 14.63, -90.51),
 # --- Oceania ---
 "canberra": ("Canberra", -35.28, 149.13), "darwin": ("Darwin", -12.46, 130.85),
 "wellington": ("Wellington", -41.29, 174.78), "noumea": ("Nouméa", -22.28, 166.46),
 "nouméa": ("Nouméa", -22.28, 166.46), "bougainville": ("Bougainville", -6.20, 155.20),
 "port moresby": ("Port Moresby", -9.44, 147.18), "honiara": ("Honiara", -9.43, 159.95),
}

# A city beats the province it sits in: "Goma in North Kivu" should pin Goma.
# Anything whose label reads as an area is ranked below a point.
_AREA_WORDS = ("state", "region", "sea", "strait", "basin", "islands", "gap", "arctic",
               "heights", "tracts", "province", "peninsula", "shoal", "circle")
_AREA_NAMES = {"Darfur", "Tigray", "Amhara", "Donbas", "Crimea", "Kashmir", "Balochistan",
               "Xinjiang", "Tibet", "Sinai", "Papua", "West Papua", "North-east Syria",
               "Puntland", "Nagorno-Karabakh", "Abkhazia", "South Ossetia", "West Bank",
               "Gaza", "Transnistria", "Ituri", "Cabo Delgado", "Mindanao",
               "North Kivu", "South Kivu", "Golan Heights", "Senkaku Islands",
               "South Lebanon", "Line of Control", "Manipur", "Assam", "Gotland", "Okinawa",
               "Guam", "Bougainville", "Kinmen", "Essequibo"}


# Cities and waterways in the same scripts, so a story about Kyiv pins on Kyiv
# rather than on the middle of Ukraine whatever language it arrived in.
PRECISE.update({
 "基輔": ("Kyiv", 50.45, 30.52), "基辅": ("Kyiv", 50.45, 30.52),
 "キーウ": ("Kyiv", 50.45, 30.52), "키이우": ("Kyiv", 50.45, 30.52),
 "Kiew": ("Kyiv", 50.45, 30.52), "Kijów": ("Kyiv", 50.45, 30.52),
 "Kiova": ("Kyiv", 50.45, 30.52), "Κίεβο": ("Kyiv", 50.45, 30.52),
 "كييف": ("Kyiv", 50.45, 30.52), "קייב": ("Kyiv", 50.45, 30.52),
 "哈爾科夫": ("Kharkiv", 49.99, 36.23), "哈尔科夫": ("Kharkiv", 49.99, 36.23),
 "ハルキウ": ("Kharkiv", 49.99, 36.23), "Charkiw": ("Kharkiv", 49.99, 36.23),
 "敖德薩": ("Odesa", 46.48, 30.73), "敖德萨": ("Odesa", 46.48, 30.73),
 "オデーサ": ("Odesa", 46.48, 30.73),
 "頓巴斯": ("Donbas", 48.30, 38.20), "顿巴斯": ("Donbas", 48.30, 38.20),
 "扎波羅熱": ("Zaporizhzhia", 47.84, 35.14), "扎波罗热": ("Zaporizhzhia", 47.84, 35.14),
 "克里米亞": ("Crimea", 45.30, 34.30), "克里米亚": ("Crimea", 45.30, 34.30),
 "クリミア": ("Crimea", 45.30, 34.30), "Krim": ("Crimea", 45.30, 34.30),
 "莫斯科": ("Moscow", 55.75, 37.62), "モスクワ": ("Moscow", 55.75, 37.62),
 "모스크바": ("Moscow", 55.75, 37.62), "Moskau": ("Moscow", 55.75, 37.62),
 "Moscou": ("Moscow", 55.75, 37.62), "Moscú": ("Moscow", 55.75, 37.62),
 "Москва": ("Moscow", 55.75, 37.62), "موسكو": ("Moscow", 55.75, 37.62),
 "加沙": ("Gaza", 31.50, 34.47), "加薩": ("Gaza", 31.50, 34.47),
 "ガザ": ("Gaza", 31.50, 34.47), "가자지구": ("Gaza", 31.50, 34.47),
 "غزة": ("Gaza", 31.50, 34.47), "עזה": ("Gaza", 31.50, 34.47),
 "Γάζα": ("Gaza", 31.50, 34.47), "กาซา": ("Gaza", 31.50, 34.47),
 "拉法": ("Rafah", 31.29, 34.25), "رفح": ("Rafah", 31.29, 34.25),
 "汗尤尼斯": ("Khan Younis", 31.34, 34.30), "خان يونس": ("Khan Younis", 31.34, 34.30),
 "特拉維夫": ("Tel Aviv", 32.08, 34.78), "特拉维夫": ("Tel Aviv", 32.08, 34.78),
 "テルアビブ": ("Tel Aviv", 32.08, 34.78), "تل أبيب": ("Tel Aviv", 32.08, 34.78),
 "耶路撒冷": ("Jerusalem", 31.78, 35.22), "エルサレム": ("Jerusalem", 31.78, 35.22),
 "예루살렘": ("Jerusalem", 31.78, 35.22), "القدس": ("Jerusalem", 31.78, 35.22),
 "德黑蘭": ("Tehran", 35.69, 51.39), "德黑兰": ("Tehran", 35.69, 51.39),
 "テヘラン": ("Tehran", 35.69, 51.39), "테헤란": ("Tehran", 35.69, 51.39),
 "طهران": ("Tehran", 35.69, 51.39), "تهران": ("Tehran", 35.69, 51.39),
 "大馬士革": ("Damascus", 33.51, 36.29), "大马士革": ("Damascus", 33.51, 36.29),
 "ダマスカス": ("Damascus", 33.51, 36.29), "دمشق": ("Damascus", 33.51, 36.29),
 "阿勒頗": ("Aleppo", 36.20, 37.13), "阿勒颇": ("Aleppo", 36.20, 37.13),
 "حلب": ("Aleppo", 36.20, 37.13),
 "貝魯特": ("Beirut", 33.89, 35.50), "贝鲁特": ("Beirut", 33.89, 35.50),
 "ベイルート": ("Beirut", 33.89, 35.50), "بيروت": ("Beirut", 33.89, 35.50),
 "薩那": ("Sanaa", 15.35, 44.21), "萨那": ("Sanaa", 15.35, 44.21),
 "صنعاء": ("Sanaa", 15.35, 44.21),
 "巴格達": ("Baghdad", 33.31, 44.36), "巴格达": ("Baghdad", 33.31, 44.36),
 "バグダッド": ("Baghdad", 33.31, 44.36), "بغداد": ("Baghdad", 33.31, 44.36),
 "喀布爾": ("Kabul", 34.53, 69.17), "喀布尔": ("Kabul", 34.53, 69.17),
 "カブール": ("Kabul", 34.53, 69.17), "كابل": ("Kabul", 34.53, 69.17),
 "喀土穆": ("Khartoum", 15.55, 32.53), "الخرطوم": ("Khartoum", 15.55, 32.53),
 "摩加迪沙": ("Mogadishu", 2.04, 45.34), "مقديشو": ("Mogadishu", 2.04, 45.34),
 "的黎波里": ("Tripoli", 32.89, 13.19), "طرابلس": ("Tripoli", 32.89, 13.19),
 "開羅": ("Cairo", 30.04, 31.24), "开罗": ("Cairo", 30.04, 31.24),
 "カイロ": ("Cairo", 30.04, 31.24), "القاهرة": ("Cairo", 30.04, 31.24),
 "安卡拉": ("Ankara", 39.93, 32.86), "アンカラ": ("Ankara", 39.93, 32.86),
 "Άγκυρα": ("Ankara", 39.93, 32.86), "أنقرة": ("Ankara", 39.93, 32.86),
 "伊斯坦布爾": ("Istanbul", 41.01, 28.98), "伊斯坦布尔": ("Istanbul", 41.01, 28.98),
 "イスタンブール": ("Istanbul", 41.01, 28.98), "إسطنبول": ("Istanbul", 41.01, 28.98),
 "台北": ("Taipei", 25.03, 121.57), "타이베이": ("Taipei", 25.03, 121.57),
 "北京": ("Beijing", 39.90, 116.41), "ペキン": ("Beijing", 39.90, 116.41),
 "베이징": ("Beijing", 39.90, 116.41), "بكين": ("Beijing", 39.90, 116.41),
 "首爾": ("Seoul", 37.57, 126.98), "首尔": ("Seoul", 37.57, 126.98),
 "서울": ("Seoul", 37.57, 126.98), "ソウル": ("Seoul", 37.57, 126.98),
 "平壤": ("Pyongyang", 39.02, 125.75), "평양": ("Pyongyang", 39.02, 125.75),
 "ピョンヤン": ("Pyongyang", 39.02, 125.75),
 "東京": ("Tokyo", 35.68, 139.69), "东京": ("Tokyo", 35.68, 139.69),
 "도쿄": ("Tokyo", 35.68, 139.69), "طوكيو": ("Tokyo", 35.68, 139.69),
 "曼谷": ("Bangkok", 13.75, 100.50), "バンコク": ("Bangkok", 13.75, 100.50),
 "방콕": ("Bangkok", 13.75, 100.50), "กรุงเทพ": ("Bangkok", 13.75, 100.50),
 "金邊": ("Phnom Penh", 11.56, 104.92), "金边": ("Phnom Penh", 11.56, 104.92),
 "พนมเปญ": ("Phnom Penh", 11.56, 104.92),
 "馬尼拉": ("Manila", 14.60, 120.98), "马尼拉": ("Manila", 14.60, 120.98),
 "マニラ": ("Manila", 14.60, 120.98),
 "雅加達": ("Jakarta", -6.21, 106.85), "雅加达": ("Jakarta", -6.21, 106.85),
 "加拉加斯": ("Caracas", 10.49, -66.88), "カラカス": ("Caracas", 10.49, -66.88),
 "太子港": ("Port-au-Prince", 18.59, -72.31),
 "波哥大": ("Bogotá", 4.71, -74.07),
 "華盛頓": ("Washington DC", 38.90, -77.04), "华盛顿": ("Washington DC", 38.90, -77.04),
 "ワシントン": ("Washington DC", 38.90, -77.04), "五角大樓": ("The Pentagon", 38.87, -77.06),
 "五角大楼": ("The Pentagon", 38.87, -77.06), "ペンタゴン": ("The Pentagon", 38.87, -77.06),
 "البنتاغون": ("The Pentagon", 38.87, -77.06),
 "倫敦": ("London", 51.51, -0.13), "伦敦": ("London", 51.51, -0.13),
 "ロンドン": ("London", 51.51, -0.13), "런던": ("London", 51.51, -0.13),
 "巴黎": ("Paris", 48.86, 2.35), "パリ": ("Paris", 48.86, 2.35), "باريس": ("Paris", 48.86, 2.35),
 "柏林": ("Berlin", 52.52, 13.40), "ベルリン": ("Berlin", 52.52, 13.40),
 "戈馬": ("Goma", -1.68, 29.23), "戈马": ("Goma", -1.68, 29.23),
 "霍爾木茲": ("Strait of Hormuz", 26.57, 56.25), "霍尔木兹": ("Strait of Hormuz", 26.57, 56.25),
 "ホルムズ": ("Strait of Hormuz", 26.57, 56.25), "호르무즈": ("Strait of Hormuz", 26.57, 56.25),
 "مضيق هرمز": ("Strait of Hormuz", 26.57, 56.25),
 "紅海": ("Red Sea", 20.00, 38.50), "红海": ("Red Sea", 20.00, 38.50),
 "紅海航線": ("Red Sea", 20.00, 38.50), "البحر الأحمر": ("Red Sea", 20.00, 38.50),
 "南シナ海": ("South China Sea", 13.00, 114.00), "南海": ("South China Sea", 13.00, 114.00),
 "남중국해": ("South China Sea", 13.00, 114.00),
 "台灣海峽": ("Taiwan Strait", 24.50, 119.50), "台湾海峡": ("Taiwan Strait", 24.50, 119.50),
 "台海": ("Taiwan Strait", 24.50, 119.50), "대만해협": ("Taiwan Strait", 24.50, 119.50),
})


def _rank(label):
    low = label.lower()
    if label in _AREA_NAMES or any(w in low for w in _AREA_WORDS):
        return 0          # an area
    return 1              # a point: city, base, facility


PRECISE_C = sorted(
    ((term, label, lat, lon, _compile(term), _rank(label))
     for term, (label, lat, lon) in PRECISE.items()),
    key=lambda row: (-row[5], -len(row[0])))   # points before areas, longest term first


# --------------------------------------------------------------------------
# Subjects
# --------------------------------------------------------------------------
TOPICS = [
    ("offensive", "Offensives & incursions", [
        ("offensive", ["military", "launched", "ground", "counter"]),
        ("invasion", ["military", "troops", "forces", "border", "full-scale"]),
        ("incursion*", None), ("crossed the border", None), ("ground operation", None),
        ("front line", None), ("frontline", None), ("advance", ["troops", "forces", "front"]),
        ("seized", ["town", "city", "village", "territory", "positions"]),
        ("captured", ["town", "city", "village", "territory", "positions"]),
        ("occupation", ["forces", "military", "territory"]), ("annex*", ["territory", "region"]),
        ("mobilis*", ["troops", "reservists"]), ("mobiliz*", ["troops", "reservists"]),
        ("ofensiva", ["militar", "lanzó"]), ("offensive militaire", None),
        ("militäroffensive", None), ("наступлени", None), ("наступ", ["військ", "росій"]),
        ("军事行动", None), ("軍事行動", None), ("軍事作戦", None), ("군사 작전", None),
    ]),
    ("strikes", "Strikes & bombardment", [
        ("airstrike*", None), ("air strike*", None), ("missile strike*", None),
        ("drone strike*", None), ("shelling", None), ("bombardment", None),
        ("artillery", None), ("rocket attack*", None), ("cruise missile*", None),
        ("ballistic missile*", None), ("loitering munition*", None), ("kamikaze drone*", None),
        ("air defence intercept*", None), ("air defense intercept*", None),
        ("frappe aérienne", None), ("luftangriff", None), ("ataque aéreo", None),
        ("ataque com drones", None), ("ракетный удар", None), ("ракетний удар", None),
        ("空襲", None), ("空爆", None), ("空袭", None), ("공습", None), ("غارة جوية", None),
        ("हवाई हमला", None), ("导弹袭击", None), ("飛彈攻擊", None), ("ミサイル攻撃", None),
    ]),
    ("civilians", "Civilians & atrocities", [
        ("civilians killed", None), ("civilian casualties", None), ("massacre*", None),
        ("hospital", ["hit", "struck", "shelled", "bombed"]),
        ("school", ["hit", "struck", "shelled", "bombed"]),
        ("market", ["bombed", "struck", "shelling"]),
        ("suicide bomb*", None), ("car bomb*", None), ("ied", ["blast", "attack", "roadside"]),
        ("terrorist attack*", None), ("claimed responsibility", None),
        ("hostage*", ["taken", "released", "held"]), ("abduct*", ["armed", "militants", "gunmen"]),
        ("gunmen", ["killed", "attacked", "opened fire"]), ("ambush", ["convoy", "patrol", "soldiers"]),
        ("mass grave*", None), ("sexual violence", ["conflict", "war", "troops"]),
        ("víctimas civiles", None), ("civils tués", None), ("zivilisten getötet", None),
        ("мирных жителей", None), ("平民 死亡", None), ("民間人 犠牲", None),
    ]),
    ("arms", "Arms industry & trade", [
        ("arms deal*", None), ("arms export*", None), ("arms sale*", None),
        ("weapons contract*", None), ("defence contract*", None), ("defense contract*", None),
        ("procurement", ["defence", "defense", "military", "weapons"]),
        ("order for", ["jets", "tanks", "missiles", "frigates", "howitzers", "drones"]),
        ("production line", ["ammunition", "missile", "shell", "artillery"]),
        ("export licen*", ["arms", "weapons", "military"]),
        ("lockheed", None), ("raytheon", None), ("rtx", ["defense", "missile"]), ("bae systems", None),
        ("rheinmetall", None), ("thales", ["defence", "defense", "missile"]), ("leonardo", ["defence", "defense"]),
        ("dassault", None), ("saab", ["gripen", "defence"]), ("kongsberg", None), ("elbit", None),
        ("hanwha", ["defense", "aerospace"]), ("norinco", None), ("rostec", None), ("hal", ["tejas", "defence"]),
        ("venta de armas", None), ("vente d'armes", None), ("rüstungsexport", None),
        ("поставки оружия", None), ("军售", None), ("武器輸出", None), ("무기 수출", None),
    ]),
    ("money", "Budgets & money", [
        ("defence budget", None), ("defense budget", None), ("military spending", None),
        ("military aid", None), ("aid package", ["military", "weapons", "security"]),
        ("supplemental", ["aid", "funding", "military"]), ("appropriation*", ["defence", "defense"]),
        ("sanction*", ["military", "arms", "defence", "weapons", "export"]),
        ("frozen assets", None), ("reconstruction fund", None), ("cost overrun", ["programme", "program", "jet", "ship"]),
        ("gasto militar", None), ("dépenses militaires", None), ("verteidigungshaushalt", None),
        ("оборонный бюджет", None), ("军费", None), ("防衛費", None), ("국방 예산", None),
    ]),
    ("bases", "Bases, forces & corridors", [
        ("military base", None), ("air base", None), ("naval base", None), ("basing agreement", None),
        ("troop deployment", None), ("deployed troops", None), ("garrison", None),
        ("port access", ["naval", "military"]), ("overflight", None), ("no-fly zone", None),
        ("exercise*", ["military", "joint", "naval", "live-fire"]), ("war games", None),
        ("peacekeep*", None), ("withdraw*", ["troops", "forces", "base"]),
        ("military corridor", None), ("supply line*", ["military", "front"]),
        ("base militaire", None), ("militärstützpunkt", None), ("военная база", None),
        ("军事基地", None), ("軍事基地", None), ("군사 기지", None),
    ]),
    ("command", "Officials & command", [
        ("defence minister", None), ("defense minister", None), ("defence secretary", None),
        ("chief of staff", ["army", "defence", "general", "military"]),
        ("general", ["appointed", "dismissed", "resigned", "commander"]),
        ("commander", ["appointed", "killed", "replaced", "dismissed"]),
        ("military coup", None), ("junta", None), ("mutiny", None),
        ("mercenar*", None), ("private military company", None), ("wagner", ["group", "mercenar"]),
        ("conscription", None), ("draft", ["military", "conscription", "mobilisation"]),
        ("desertion", ["soldiers", "troops"]), ("court martial", None),
        ("ministro de defensa", None), ("ministre de la défense", None), ("verteidigungsminister", None),
        ("министр обороны", None), ("国防部长", None), ("防衛大臣", None), ("국방부 장관", None),
    ]),
    ("law", "Law & accountability", [
        ("war crimes", None), ("crimes against humanity", None), ("genocide", ["case", "court", "inquiry", "charges"]),
        ("icc", ["warrant", "prosecutor", "charges", "court"]), ("international criminal court", None),
        ("international court of justice", None), ("arrest warrant", None),
        ("commission of inquiry", None), ("tribunal", ["war", "crimes", "conflict"]),
        ("ceasefire", None), ("truce", None), ("peace deal", None), ("peace talks", None),
        ("prisoner exchange", None), ("geneva convention*", None), ("arms embargo", None),
        ("alto el fuego", None), ("cessez-le-feu", None), ("waffenstillstand", None),
        ("прекращение огня", None), ("停火", None), ("停戦", None), ("휴전", None),
    ]),
    ("nuclear", "Nuclear & strategic", [
        ("nuclear warhead*", None), ("nuclear weapon*", None), ("nuclear test*", None),
        ("nuclear doctrine", None), ("icbm", None), ("hypersonic", None),
        ("missile test", None), ("enrichment", ["uranium", "nuclear", "inspectors"]),
        ("iaea", None), ("npt", ["treaty", "nuclear"]), ("new start", ["treaty"]),
        ("arms control", None), ("deterrence", None), ("submarine", ["ballistic", "nuclear", "patrol"]),
        ("armas nucleares", None), ("arme nucléaire", None), ("atomwaffen", None),
        ("ядерное оружие", None), ("核武器", None), ("核兵器", None), ("핵무기", None),
    ]),
    ("humanitarian", "Humanitarian consequence", [
        ("displaced", ["fighting", "conflict", "war", "offensive", "shelling"]),
        ("refugee*", ["fleeing", "conflict", "war", "border", "camp"]),
        ("famine", ["conflict", "war", "siege", "blockade"]),
        ("siege", None), ("blockade", None), ("aid convoy", None), ("humanitarian corridor", None),
        ("aid blocked", None), ("hunger", ["conflict", "war", "siege"]),
        ("casualt*", ["toll", "civilian", "reported"]), ("death toll", None),
        ("hospitals overwhelmed", None), ("cholera", ["conflict", "displacement", "camp"]),
        ("desplazados", ["conflicto", "violencia"]), ("déplacés", ["conflit", "combats"]),
        ("вынужденные переселенцы", None), ("难民", None), ("避難民", None),
    ]),
    ("cyber", "Cyber & information", [
        ("cyberattack*", ["military", "government", "infrastructure", "defence", "grid"]),
        ("hacked", ["ministry", "defence", "military", "government"]),
        ("disinformation campaign", None), ("information operation*", None),
        ("jamming", ["gps", "signal", "satellite"]), ("electronic warfare", None),
        ("surveillance", ["military", "drone", "satellite", "intelligence"]),
        ("intelligence agency", None), ("espionage", None), ("spy", ["arrested", "agency", "charges"]),
        ("кибератака", None), ("网络攻击", None), ("サイバー攻撃", None),
    ]),
]

# --------------------------------------------------------------------------
# The gate.
#
# MILITARY — the story concerns armed force, the institutions that hold it, or
#            the trade and money attached to it, in any of the feed's languages.
# BLOCK    — the metaphors. "Price war", "battle for market share", "attack" in
#            sport, the shooter franchises: this vocabulary is used constantly
#            for things that are not war, and without a block list they dominate.
# --------------------------------------------------------------------------
MILITARY = [
    "military", "armed forces", "army", "navy", "air force", "troops", "soldier*",
    "battalion", "brigade", "regiment", "garrison", "combatant*", "militia*", "insurgen*",
    "guerrilla*", "paramilitar*", "mercenar*", "armed group*", "armed conflict", "warfare",
    "war", "civil war", "fighting", "clashes", "hostilities", "combat", "firefight",
    "front line", "frontline", "offensive", "invasion", "incursion*",
    "occupation", "annexation", "ceasefire", "truce", "armistice", "peacekeep*",
    "airstrike*", "air strike*", "missile*", "rocket*", "artillery", "shelling", "bombardment",
    "drone strike*", "warplane*", "fighter jet*", "warship*", "submarine", "tank*", "howitzer*",
    "munition*", "ammunition", "weapon*", "arms deal*", "arms export*", "arms sale*",
    "defence ministry", "defense ministry", "defence budget", "defense budget",
    "defence contract*", "defense contract*", "nato", "pentagon", "general staff",
    "war crimes", "terrorist attack*", "suicide bomb*", "car bomb*", "ied",
    "nuclear weapon*", "nuclear warhead*", "icbm", "arms control", "arms embargo",
    "coup", "junta", "conscription", "mobilisation", "mobilization",
    "militar*", "ejército", "fuerzas armadas", "ofensiva", "guerra", "alto el fuego",
    "militaire", "armée", "forces armées", "guerre", "cessez-le-feu", "frappe",
    "militär*", "bundeswehr", "krieg", "waffenstillstand", "luftangriff", "rüstung*",
    "esercito", "guerra", "militare", "leger", "oorlog", "krig", "wojsko", "wojna",
    "армия", "войск", "военн", "война", "перемирие", "ракет", "обстрел",
    "армія", "військов", "війна", "ordu", "askeri", "savaş", "ateşkes",
    "جيش", "عسكري", "حرب", "قصف", "وقف إطلاق النار", "ارتش", "نظامی", "جنگ",
    "सेना", "सैन्य", "युद्ध", "हमला सैनिक", "সেনা", "সামরিক", "যুদ্ধ",
    "militer", "tentara", "perang", "quân sự", "quân đội", "chiến tranh",
    "ทหาร", "กองทัพ", "สงคราม", "โจมตีทางอากาศ", "หยุดยิง",
    "軍", "軍隊", "戦争", "自衛隊", "空襲", "空爆", "停戦", "ミサイル", "軍事",
    "军队", "军事", "战争", "空袭", "停火", "导弹", "军售",
    "군", "군사", "전쟁", "공습", "휴전", "미사일", "στρατ*", "πόλεμος", "צבא", "מלחמה",
    "jeshi", "vita vya", "shambulio la anga",
]

BLOCK = [
    # the metaphors, which are relentless
    "price war", "bidding war", "culture war", "war of words", "trade war", "turf war",
    "battle for market share", "battleground state", "war chest", "war room",
    "attack on the record", "under fire over comments", "war on drugs prices",
    "tug of war", "war paint", "fantasy football", "war memorial ceremony gift",
    # sport
    "premier league", "champions league", "nfl", "nba", "mlb", "world cup match",
    "boxing", "ufc", "wrestling", "esports", "counter-strike", "call of duty",
    "battlefield game", "war thunder", "video game", "box office", "season finale",
    # commerce and horoscopes
    "gift guide", "best deals", "coupon", "horoscope", "astrolog*", "zodiac", "tarot",
]

# --------------------------------------------------------------------------
# Direction: escalation, or a step away from it.
# --------------------------------------------------------------------------
ESCALATION = [
    "launched an offensive", "offensive launched", "invaded", "crossed the border",
    "airstrike", "missile strike", "drone strike", "shelling", "bombardment", "seized",
    "captured", "killed", "casualties", "deployed", "reinforcement*", "escalat*",
    "mobilis*", "canceled talks", "cancelled talks", "walked out of talks", "ultimatum",
    "arms deal signed", "weapons delivered", "budget increase", "test-fired", "test fired",
    "state of emergency", "martial law", "coup", "sanctions imposed", "blockade",
]
DEESCALATION = [
    "ceasefire", "truce", "armistice", "peace deal", "peace talks", "agreement reached",
    "withdraw*", "pullback", "pull back", "prisoner exchange", "prisoner swap",
    "aid corridor", "humanitarian corridor", "aid convoy allowed", "de-escalat*",
    "arms control agreement", "treaty signed", "inspectors admitted", "sanctions lifted",
    "released detainees", "resumed talks", "mediation", "negotiations opened",
    "alto el fuego", "cessez-le-feu", "waffenstillstand", "перемирие", "停火", "停戦", "휴전",
]

# --------------------------------------------------------------------------
# Evidence signals for the pressure score.
# --------------------------------------------------------------------------
DOCUMENTED = [
    "confirmed", "said in a statement", "announced", "signed", "approved", "awarded",
    "carried out", "struck", "killed", "wounded", "captured", "seized", "arrested",
    "charged", "verdict", "ruling", "warrant issued", "resolution adopted",
    "imposed sanctions", "delivered", "test-fired", "deployed", "withdrew",
]
INSTITUTIONAL = [
    "united nations", "security council", "ohchr", "icrc", "icc", "international court",
    "nato", "european union", "sipri", "acled", "iiss", "crisis group", "human rights watch",
    "amnesty international", "bellingcat", "airwars", "ministry of defence",
    "ministry of defense", "pentagon", "general staff", "official figures", "peer-reviewed",
    "commission of inquiry", "monitoring group", "satellite imagery",
]
MEASURED = [
    "killed", "wounded", "casualties", "death toll", "per cent", "percent", "%",
    "billion", "million", "kilometres", "kilometers", "troops", "systems", "rounds",
    "warheads", "aircraft", "vehicles", "displaced", "thousands of", "hundreds of",
]
PROJECTED = [
    "planned", "expected", "scheduled", "deadline", "talks due", "will begin",
    "by 2030", "next year", "in the coming weeks", "under negotiation", "proposed",
    "draft resolution", "pending approval",
]


MILITARY_C = _compile_all(MILITARY)
BLOCK_C = _compile_all(BLOCK)
ESCALATION_C = _compile_all(ESCALATION)
DEESCALATION_C = _compile_all(DEESCALATION)
DOCUMENTED_C = _compile_all(DOCUMENTED)
INSTITUTIONAL_C = _compile_all(INSTITUTIONAL)
MEASURED_C = _compile_all(MEASURED)
PROJECTED_C = _compile_all(PROJECTED)
TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]
GEO3_C = [(rid, rlabel, [(sid, slabel, [(pid, plabel, _compile_all(terms))
                                        for pid, plabel, terms in places])
                        for sid, slabel, places in subs])
          for rid, rlabel, subs in GEO3]


def relevant(text):
    """Armed force, the institutions that hold it, or the trade and money
    attached to it. The metaphors are refused."""
    if hit(text, BLOCK_C):
        return False
    return hit(text, MILITARY_C)


def kind_of(text):
    """Escalation, a step away from it, or both in one story."""
    kinds = []
    if hit(text, ESCALATION_C):
        kinds.append("escalation")
    if hit(text, DEESCALATION_C):
        kinds.append("deescalation")
    return kinds or ["escalation"]


def pressure(text, standing, placed):
    total, reasons = 0, []
    if hit(text, DOCUMENTED_C):
        total += 2
        reasons.append("documented")
    if hit(text, INSTITUTIONAL_C):
        total += 2
        reasons.append("institutional")
    if hit(text, MEASURED_C):
        total += 1
        reasons.append("measured")
    if hit(text, PROJECTED_C):
        total += 1
        reasons.append("projected")
    if placed:
        total += 1
        reasons.append("located")
    if standing in ("official", "research"):
        total += 1
        reasons.append("primary source")
    return total, reasons


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


def places_for(text):
    """Returns (regions, subregions, places). Naming a place implies the
    subregion and region above it."""
    regions, subs, places = [], [], []
    for rid, _rl, sublist in GEO3_C:
        for sid, _sl, plist in sublist:
            for pid, _pl, terms in plist:
                if not hit(text, terms):
                    continue
                if pid not in places:
                    places.append(pid)
                if sid not in subs:
                    subs.append(sid)
                if rid not in regions:
                    regions.append(rid)
    return (regions or ["unlocated"], subs or ["unlocated"], places or ["unlocated"])


def precise_for(text):
    """A city, province, base or waterway named in the story. Checked before the
    country layer so a headline about Kharkiv is pinned on Kharkiv rather than
    the middle of Ukraine. Longest term wins."""
    for term, label, lat, lon, rx, _rk in PRECISE_C:
        if hit(text, [rx]):
            return label, [lat, lon]
    return None, None



# --------------------------------------------------------------------------
# Which country a story is IN, not merely which countries it names.
#
# "Krieg im Libanon: Tote bei israelischem Luftangriff" names Lebanon and
# Israel. Taking whichever comes first in the gazetteer put the pin on Israel,
# which is the actor, not the place. Almost every language marks the location
# with a preposition or particle immediately before the name - im, in, en, à,
# sur, в, на, στη, في, ב, ที่, 在 - so a name carrying one of those is treated
# as the scene and wins. Nothing marked means nothing changes: the old order
# still decides, so this can only move a pin off an actor and onto a place.
# --------------------------------------------------------------------------
LOCATIVE = [
 " in ", " im ", " en ", " au ", " aux ", " a ", " à ", " al ", " nel ", " nella ",
 " on ", " over ", " near ", " into ", " inside ", " across ", " throughout ",
 " sur ", " dans ", " van ", " naar ", " uit ", " w ", " na ", " do ", " em ", " no ",
 " v ", " в ", " на ", " у ", " до ", " στη", " στο", " την ", " στην ",
 "في ", "ب", "ל", "ב", "ที่", "ใน", "在", "で", "へ", "에서", "로",
]
_LOC_MAX = 12          # how far back to look for the marker


def _first_pos(text, compiled):
    """Where a place's terms first appear in the text, or None."""
    best = None
    for c in compiled:
        if isinstance(c, str):
            i = text.find(c)
        else:
            mo = c.search(text)
            i = mo.start() if mo else -1
        if i >= 0 and (best is None or i < best):
            best = i
    return best


def _is_scene(text, pos):
    """True when the name at this position is preceded by a locative marker."""
    if pos is None:
        return False
    window = text[max(0, pos - _LOC_MAX):pos]
    return any(mark in window for mark in LOCATIVE)


def scene_first(text, places):
    """Reorder matched places so any marked as the scene of the story lead."""
    if len(places) < 2:
        return places
    terms = {}
    for _rid, _rl, sublist in GEO3_C:
        for _sid, _sl, plist in sublist:
            for pid, _pl, compiled in plist:
                if pid in places:
                    terms[pid] = compiled
    scene, rest = [], []
    for pid in places:
        (scene if _is_scene(text, _first_pos(text, terms.get(pid, []))) else rest).append(pid)
    return scene + rest


def point_for(text, places, subs, regions):
    """The most specific point a story resolved to: a named sub-national place
    if there is one, otherwise the country, otherwise the subregion or region.
    Returns (label_or_None, point_or_None)."""
    label, point = precise_for(text)
    if point:
        return label, point
    places = scene_first(text, places)
    for level in (places, subs, regions):
        for pid in level:
            if pid in COORDS:
                return None, COORDS[pid]
    return None, None


def load_sources():
    with open(SOURCES_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)
    srcs = []
    for s in cfg.get("direct", []):
        srcs.append({"name": s["name"], "lang": s["lang"], "region": s["region"],
                     "kind": s.get("kind", "news"), "url": s["url"]})
    # The wire's own group is provenance, never geography — where a story
    # happens is decided by the gazetteer, not by which edition carried it.
    for block, prefix, group, kind in (("gnews", "Google News · ", "Regional press", "news"),
                                       ("scope", "Planet scale · ", "Planet scale", "scope")):
        for loc in cfg.get(block, []):
            srcs.append({"name": prefix + loc["label"], "lang": loc["lang"],
                         "region": group, "kind": kind,
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
                row["w"], row["sr"], row["pl"] = places_for(text)
                row["pn"], row["ll"] = point_for(text, row["pl"], row["sr"], row["w"])
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
        languages.setdefault(loc["lang"], re.sub(r"\s*·.*$|\s*\(.*$|\s+\d+$", "", loc["label"]).strip())
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
        "coords": COORDS,
        "geo": ([{"id": rid, "label": rlabel,
                  "subs": [{"id": sid, "label": slabel,
                            "places": [{"id": pid, "label": plabel} for pid, plabel, _t in places]}
                           for sid, slabel, places in subs]}
                 for rid, rlabel, subs in GEO3] +
                [{"id": "unlocated", "label": "No single region", "subs": []}]),
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
