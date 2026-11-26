"""
Diagnostic RNSR / ROR / annuaire CNRS / paysage sur les structures de recherche actives.

Premier pas vers un référentiel des structures de recherche construit à partir de
plusieurs sources. Ce module ne fusionne rien : il relie les fiches entre elles et
mesure où et comment les sources divergent.

Étapes :
0. chargement des dumps (S3 scanR, lus en mémoire, sans cache) et des liens IdRef (SPARQL)
1. adaptateurs : chaque fiche source est traduite dans un schéma pivot commun.
   Les tutelles CNRS sont des libellés courts (« UNIV PARIS-SACLAY ») : elles sont
   rapprochées des ids paysage par libellé exact, sinon par co-occurrence avec les
   tutelles RNSR des unités liées (indice de Jaccard) (cnrs_tutelles_mapping.csv)
2. périmètre ROR : fiches actives, avec un parent (ou un grand-parent) établissement
   français de l'ESR, et qui ne sont pas elles-mêmes un établissement. Le pays du labo
   n'est pas filtré (IRL, sites à l'étranger) ; celui de l'établissement l'est, sinon
   une tutelle étrangère d'une IRL ferait entrer toutes ses composantes
3. liens : graphe d'identifiants (IdRef, paysage, RNSR déclaré dans l'annuaire CNRS,
   codes d'unité confirmés, sigle + tutelle + ville) et composantes connexes (= clusters).
   La ville se juge sur la commune ou l'unité urbaine (référentiel géographique MESR) :
   Grenoble et Saint-Martin-d'Hères sont « même ville ».
4. diagnostic : catégories de clusters, sources des liens, désaccords par champ,
   et vue par tutelle (unités vues par le RNSR et/ou par ROR, tutelle référente)

Périmètre : RNSR actifs. Les RNSR inactifs sont gardés pour l'appariement (une fiche
ROR liée à une unité fermée n'est pas une « ROR hors RNSR »).

Tutelle référente : lue dans le champ `referent` des institutions de rnsr-v2. Tant que
le champ est absent du dump, les colonnes correspondantes restent vides.

Usage local :
    python -m project.server.main.diagnostic_rnsr --output-dir /tmp/diag/out
"""
import argparse
import csv
import gzip
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

import requests
from unidecode import unidecode

from project.server.main.logger import get_logger

logger = get_logger(__name__)

S3_BASE_URL = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production'
DUMP_FILES = {
    'rnsr': 'rnsr-v2.jsonl',
    'ror': 'ror.jsonl.gz',
    'paysage': 'paysage_dump.jsonl.gz',
    'cnrs': 'cnrs_annuaire.jsonl.gz',
}
DEFAULT_OUTPUT_DIR = '/upw_data/scanr/orga_ref/diagnostic_rnsr/output'

ROR_REGEX = re.compile(r'^0[a-z0-9]{6}[0-9]{2}$')

# Relation RNSR prise en compte comme tutelle (on écarte « établissement participant »)
RNSR_TUTELLE_TYPE = 'établissement tutelle'

# Catégories paysage considérées comme établissements de l'ESR
ESR_PAYSAGE_CATEGORIES = {
    'mCpLW',  # Université
    'J0mQ2',  # Grand établissement
    'hg3v5',  # École d'ingénieurs
    '2ZdzP',  # Organisme de recherche
    'vt1Z4',  # Institut ou école extérieur aux universités
    '1uQLb',  # Centre hospitalier
    'v1vFV',  # Regroupement universitaire
    '2f1dX',  # Autre établissement d'enseignement supérieur
    'UfEnK',  # Établissement d'enseignement supérieur privé d'intérêt général
    'WCat8',  # Établissements publics relevant du ministre chargé de l'ESR
    '2SZlU',  # Principaux établissements d'enseignement supérieur
}
# Catégorie principale paysage qui exclut une tutelle RNSR de la liste des établissements ESR
NON_ESR_MAIN_CATEGORIES = {
    's79DJ',  # Entreprise
    '2fy6x',  # Entreprise (ROR)
    'rh13d',  # Start-Up
}

# Types ROR compatibles avec une structure de recherche
ROR_LAB_TYPES = {'facility', 'education', 'healthcare', 'nonprofit', 'other'}
# Profondeur max de remontée des parents ROR jusqu'à un établissement ESR
MAX_PARENT_DEPTH = 2

# Codes d'unité : préfixe -> famille. Les numéros sont parfois conservés lors d'un
# changement de préfixe (EA -> UR, UMS -> UAR...), d'où le regroupement en familles.
CODE_FAMILIES = {
    'UMR': 'UMR', 'UMRS': 'UMR', 'UMR_S': 'UMR', 'UMR-S': 'UMR', 'U': 'UMR',
    'UMR_A': 'UMR', 'UMR_D': 'UMR', 'UMR_E': 'UMR', 'UMR_MA': 'UMR', 'UMRA': 'UMR', 'UMRD': 'UMR',
    'UR': 'UR', 'EA': 'UR', 'ULR': 'UR', 'URP': 'UR',
    'UAR': 'UAR', 'UMS': 'UAR', 'USR': 'UAR', 'UPS': 'UAR', 'US': 'UAR',
    'FR': 'FR', 'FED': 'FR', 'IFR': 'FR',
    'IRL': 'IRL', 'UMI': 'IRL',
    'UPR': 'UPR', 'FRE': 'FRE', 'GDR': 'GDR', 'ERL': 'ERL', 'EMR': 'EMR', 'UE': 'UE',
    'CIC': 'CIC', 'JE': 'JE', 'USC': 'USC',
}
_CODE_PREFIXES = sorted(CODE_FAMILIES, key=len, reverse=True)
CODE_REGEX = re.compile(
    r'(?<![A-Za-z0-9])(' + '|'.join(re.escape(p) for p in _CODE_PREFIXES) + r')\s*[-_ ]?\s*(\d{2,5})(?![0-9])'
)
LABEL_SIMILARITY_CONFIRM = 0.75
LABEL_SIMILARITY_CLOSE = 0.8
LABEL_SIMILARITY_ACRONYM = 0.5   # lien par sigle : libellés seulement « pas trop éloignés »
# Rapprochement libellé de tutelle CNRS -> paysage : Jaccard minimal entre les unités
# portant le libellé et celles dont le RNSR porte l'id paysage
CNRS_TUTELLE_MIN_JACCARD = 0.5

TODAY = str(datetime.today())[0:10]


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------

def normalize_text(text) -> str:
    if not isinstance(text, str):
        return ''
    text = unidecode(text).lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def normalize_city(city) -> str:
    city = normalize_text(city)
    city = re.sub(r'\bcedex\b.*$', '', city)
    city = re.sub(r'\d+', '', city)
    city = re.sub(r'\bst\b', 'saint', city)
    city = re.sub(r'\bste\b', 'sainte', city)
    city = re.sub(r'\blez\b', 'les', city)
    # article en tête : « Le Kremlin-Bicêtre » = « Kremlin-Bicêtre »
    city = re.sub(r'^(le|la|les|l) ', '', city)
    # arrondissements : « Paris 5e », « Lyon 3eme arrondissement », « Marseille 13e »
    city = re.sub(r'(\s+(e|er|eme|ème|arrondissement|arr))+$', '', city)
    return re.sub(r'\s+', ' ', city).strip()


# ---------------------------------------------------------------------------
# Géographie : commune et unité urbaine (référentiel géographique MESR / INSEE)
# ---------------------------------------------------------------------------
# « même ville » se juge sur la commune, ou sur l'unité urbaine pour les campus
# en périphérie (Grenoble <-> Saint-Martin-d'Hères). Attention, l'unité urbaine de
# Paris est très large (Orsay, Gif, Saclay, Palaiseau en font partie).

GEO_REF_URL = 'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-referentiel-geographique/download/?format=csv'
# Paris, Lyon, Marseille : les arrondissements sont rattachés à la commune
PLM = (('751', '75056'), ('6938', '69123'), ('132', '13055'))
FRENCH_COUNTRIES = {None, '', 'fr', 'fra', 'france'}
# lieux usuels qui ne sont pas des communes -> commune de référence (même unité urbaine)
PLACE_ALIASES = {
    'sophia antipolis': 'valbonne',
    'marne la vallee': 'champs sur marne',
    'saint quentin en yvelines': 'guyancourt',
    'paris saclay': 'saclay',
}


def load_geo_ref() -> Optional[dict]:
    """
    Index nom de commune normalisé -> communes candidates {com, uu, dep, lat, lon}.
    Les anciens noms et les communes fusionnées pointent vers la commune actuelle.
    Renvoie None si le référentiel est indisponible (comparaison sur le nom seul).
    """
    try:
        import pandas as pd
        url = GEO_REF_URL + (f"&apikey={os.getenv('ODS_API_KEY')}" if os.getenv('ODS_API_KEY') else '')
        df = pd.read_csv(url, sep=';', dtype=str).fillna('')
    except Exception as e:
        logger.warning(f'référentiel géographique indisponible ({e}) : villes comparées sur le nom')
        return None
    current = {}
    for row in df.to_dict(orient='records'):
        if not row['com_nom_fin']:
            current[row['com_code']] = row

    def commune(row):
        if row['com_nom_fin']:
            # commune disparue : rattachée à la commune actuelle, ignorée sinon
            if row['com_code_actuel'] not in current:
                return None
            row = current[row['com_code_actuel']]
        code = row['com_code']
        for prefix, plm in PLM:
            if code.startswith(prefix) and len(code) == 5 and code != plm:
                code = plm
        lat, lon = (row['geolocalisation'].split(',') + [''])[0:2] if row['geolocalisation'] else ('', '')
        return {'com': code, 'uu': row['uucr_id'] if row['uucr_id'].startswith('UU') else None,
                'dep': row['dep_code'], 'lat': float(lat) if lat else None, 'lon': float(lon) if lon.strip() else None}

    index = defaultdict(dict)
    for row in df.to_dict(orient='records'):
        c = commune(row)
        if c is None:
            continue
        names = [row['com_nom']] + [n for n in row['com_nom_anciens'].split(';') if n]
        for name in names:
            index[normalize_city(name)].setdefault(c['com'], c)
    logger.debug(f'référentiel géographique : {len(index)} noms de communes')
    return {k: list(v.values()) for k, v in index.items()}


def postcode_dep(postcode) -> Optional[str]:
    pc = re.sub(r'\D', '', str(postcode or ''))
    if len(pc) != 5:
        return None
    if pc.startswith('97') or pc.startswith('98'):
        return pc[0:3]
    if pc.startswith('20'):
        return '2A' if int(pc) < 20200 else '2B'
    return pc[0:2]


def resolve_place(city, postcode=None, country=None, gps=None, geo_ref: Optional[dict] = None) -> Optional[dict]:
    """
    Commune et unité urbaine d'une fiche : {'com', 'uu', 'name'}.
    Homonymes départagés par le code postal, sinon par les coordonnées GPS.
    À l'étranger ou hors référentiel : le nom normalisé sert de commune.
    """
    name = normalize_city(city)
    if not name:
        return None
    name = PLACE_ALIASES.get(name, name)
    fallback = {'com': f'nom:{name}', 'uu': None, 'name': name}
    if geo_ref is None or str(country or '').strip().lower() not in FRENCH_COUNTRIES:
        return fallback
    candidates = geo_ref.get(name) or []
    dep = postcode_dep(postcode)
    if dep and len(candidates) > 1:
        candidates = [c for c in candidates if c['dep'] == dep] or candidates
    if len(candidates) > 1 and gps and gps.get('lat') is not None:
        located = [c for c in candidates if c['lat'] is not None]
        if located:
            candidates = [min(located, key=lambda c: (c['lat'] - gps['lat']) ** 2 + (c['lon'] - gps['lon']) ** 2)]
    if len(candidates) != 1:
        # homonymes non départagés : même unité urbaine pour tous, sinon nom seul
        uus = {c['uu'] for c in candidates}
        return dict(fallback, uu=uus.pop() if len(uus) == 1 and candidates else None)
    c = candidates[0]
    return {'com': c['com'], 'uu': c['uu'], 'name': name}


def place_relation(a: Optional[dict], b: Optional[dict]) -> str:
    """same_city / same_uu / different / unknown."""
    if not a or not b:
        return 'unknown'
    # même nom : même ville (une des deux fiches n'a pas pu être située, homonymes)
    if a['com'] == b['com'] or a['name'] == b['name']:
        return 'same_city'
    if a['uu'] and a['uu'] == b['uu']:
        return 'same_uu'
    return 'different'


def records_place_relation(a: dict, b: dict) -> str:
    return place_relation(a.get('place'), b.get('place'))


CITY_OK = {'same_city', 'same_uu'}


def normalize_url(url) -> tuple:
    """Renvoie (domaine, url normalisée) ou (None, None)."""
    if not isinstance(url, str) or not url.strip():
        return None, None
    url = url.strip().lower()
    url = re.sub(r'^[a-z]+://', '', url)
    url = re.sub(r'^www\d?\.', '', url)
    url = url.split('#')[0].rstrip('/')
    domain = url.split('/')[0]
    return domain, url


def similarity(a: str, b: str) -> float:
    a, b = normalize_text(a), normalize_text(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def extract_codes(texts) -> set:
    """Codes d'unité normalisés (famille, numéro) trouvés dans une liste de textes."""
    codes = set()
    for text in texts:
        if not isinstance(text, str):
            continue
        for prefix, number in CODE_REGEX.findall(text.upper()):
            codes.add((CODE_FAMILIES[prefix], str(int(number))))
    return codes


def normalize_ror(ror) -> Optional[str]:
    if not isinstance(ror, str):
        return None
    ror = ror.strip().lower().split('/')[-1]
    return ror if ROR_REGEX.match(ror) else None


def write_jsonl(data: list, path: str) -> None:
    with open(path, 'w') as f:
        for elt in data:
            f.write(json.dumps(elt, ensure_ascii=False, default=list) + '\n')


def write_csv(rows: list, path: str) -> None:
    if not rows:
        open(path, 'w').close()
        return
    fieldnames = []
    for row in rows:
        for k in row:
            if k not in fieldnames:
                fieldnames.append(k)
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: '|'.join(map(str, v)) if isinstance(v, (list, set, tuple)) else v
                             for k, v in row.items()})


# ---------------------------------------------------------------------------
# Étape 0 : chargement (sans cache : dumps lus en mémoire, IdRef interrogé à chaque fois)
# ---------------------------------------------------------------------------

def fetch_dump(name: str) -> list:
    url = f'{S3_BASE_URL}/{DUMP_FILES[name]}'
    logger.debug(f'{name} : téléchargement {url}')
    r = requests.get(url, timeout=600)
    r.raise_for_status()
    content = gzip.decompress(r.content) if url.endswith('.gz') else r.content
    return [json.loads(line) for line in content.decode('utf-8').splitlines() if line.strip()]


def load_idref_links() -> Optional[dict]:
    """
    Liens RNSR -> PIDs depuis IdRef (vip.get_rnsr_pids_from_idref).
    Renvoie None si le SPARQL est indisponible (repli sur les ROR de rnsr-v2).
    """
    try:
        from project.server.main.vip import get_rnsr_pids_from_idref
        pids = get_rnsr_pids_from_idref()
    except Exception as e:
        logger.warning(f'idref : SPARQL indisponible ({e}), repli sur les ROR du dump RNSR')
        return None
    logger.debug(f'idref : {len(pids)} RNSR avec PIDs')
    return pids


# ---------------------------------------------------------------------------
# Étape 1 : adaptateurs vers le schéma pivot
# ---------------------------------------------------------------------------

def rnsr_to_pivot(elt: dict) -> dict:
    label = (elt.get('label') or {}).get('fr') or (elt.get('label') or {}).get('default')
    acronym = (elt.get('acronym') or {}).get('fr') or (elt.get('acronym') or {}).get('default')
    ext = elt.get('externalIds') or []
    code_texts = [e.get('id') for e in ext if e.get('type') == 'label_numero'] + [acronym]
    address = (elt.get('address') or [{}])[0] or {}
    website = None
    for link in elt.get('links') or []:
        if isinstance(link, dict) and link.get('url'):
            website = link['url']
            break
    tutelles, participants = [], []
    for inst in elt.get('institutions') or []:
        if not isinstance(inst, dict) or not isinstance(inst.get('structure'), str):
            continue
        if isinstance(inst.get('endDate'), str) and inst['endDate'][0:10] < TODAY:
            continue
        # referent : True / False, ou None tant que le champ n'est pas dans le dump
        entry = {'id': inst['structure'].strip(), 'label': inst.get('label'),
                 'referent': inst.get('referent')}
        if inst.get('relationType') == RNSR_TUTELLE_TYPE:
            tutelles.append(entry)
        else:
            participants.append(entry)
    return {
        'source': 'rnsr',
        'id': elt['id'].strip().upper(),
        'active': elt.get('status') == 'active',
        'level': elt.get('level'),
        'label': label,
        'acronym': acronym,
        'aliases': elt.get('alias') or [],
        'codes': extract_codes(code_texts),
        'start_year': str(elt['startDate'])[0:4] if isinstance(elt.get('startDate'), str) else None,
        'end_date': elt.get('endDate') if isinstance(elt.get('endDate'), str) else None,
        'city': address.get('city'),
        'postcode': address.get('postcode'),
        'website': website,
        'tutelles': tutelles,
        'referent_known': any(t['referent'] is not None for t in tutelles),
        'referents': [t['id'] for t in tutelles if t['referent'] is True],
        'participants': participants,
        'rors_from_dump': sorted({normalize_ror(e.get('id')) for e in ext
                                  if e.get('type') == 'ror' and normalize_ror(e.get('id'))}),
        'idref': [e.get('id') for e in ext if e.get('type') == 'idref'],
        'hal': [e.get('id') for e in ext if e.get('type') == 'hal'],
        'label_numero': [e.get('id') for e in ext if e.get('type') == 'label_numero'],
        'country': address.get('iso3'),
        'address': address.get('address'),
        'leaders': [{'role': l.get('role'), 'name': ' '.join(x for x in (l.get('firstName'), l.get('lastName')) if x),
                     'prenom': l.get('firstName'), 'nom': l.get('lastName'),
                     'idref': (l.get('person') or '').replace('idref', '') or None, 'start': (l.get('startDate') or '')[0:10]}
                    for l in elt.get('leaders') or []
                    if isinstance(l, dict) and not (isinstance(l.get('endDate'), str) and l['endDate'][0:10] < TODAY)],
        'predecessors': [p.get('structure') for p in elt.get('predecessors') or [] if isinstance(p, dict)],
    }


def ror_to_pivot(elt: dict) -> dict:
    names = elt.get('names') or []
    label_display = next((n['value'] for n in names if 'ror_display' in n.get('types', [])), None)
    labels = {}
    for n in names:
        if 'label' in n.get('types', []) and n.get('lang') in ('fr', 'en'):
            labels.setdefault(n['lang'], n['value'])
    acronyms = [n['value'] for n in names if 'acronym' in n.get('types', [])]
    all_names = [n['value'] for n in names if isinstance(n.get('value'), str)]
    location = (elt.get('locations') or [{}])[0] or {}
    geo = location.get('geonames_details') or {}
    websites = [link.get('value') for link in elt.get('links') or [] if link.get('type') == 'website']
    relations = defaultdict(list)
    for rel in elt.get('relationships') or []:
        rel_id = normalize_ror(rel.get('id'))
        if rel_id:
            relations[rel.get('type')].append(rel_id)
    external_ids = {}
    for e in elt.get('external_ids') or []:
        if isinstance(e.get('all'), list):
            external_ids[e.get('type')] = e['all']
    established = elt.get('established')
    return {
        'source': 'ror',
        'id': normalize_ror(elt['id']),
        'status': elt.get('status'),
        'active': elt.get('status') == 'active',
        'types': elt.get('types') or [],
        'country': geo.get('country_code'),
        'label': label_display,
        'label_fr': labels.get('fr'),
        'label_en': labels.get('en'),
        'acronyms': acronyms,
        'names': all_names,
        'codes': extract_codes(all_names),
        'start_year': str(int(established)) if isinstance(established, (int, float)) and established == established else None,
        'city': geo.get('name'),
        'gps': {'lat': geo.get('lat'), 'lon': geo.get('lng')} if geo.get('lat') is not None else None,
        'website': websites[0] if websites else None,
        'parents': relations.get('parent', []),
        'children': relations.get('child', []),
        'related': relations.get('related', []),
        'predecessors': relations.get('predecessor', []),
        'successors': relations.get('successor', []),
        'external_ids': external_ids,
        'last_modified': ((elt.get('admin') or {}).get('last_modified') or {}).get('date'),
    }


def paysage_to_pivot(elt: dict) -> dict:
    identifiers = defaultdict(list)
    for i in elt.get('identifiers') or []:
        if i.get('active') is False or not isinstance(i.get('value'), str):
            continue
        identifiers[i.get('type')].append(i['value'].strip())
    rors = sorted({normalize_ror(r) for r in identifiers.get('ror', []) if normalize_ror(r)})
    rnsrs = sorted({r.upper() for r in identifiers.get('rnsr', [])})
    localisation = elt.get('currentLocalisation') or {}
    name = elt.get('currentName') or {}
    sirets = identifiers.get('siret', [])
    return {
        'source': 'paysage',
        'id': elt['id'],
        'active': elt.get('status') == 'active',
        'label': elt.get('displayName'),
        'acronym': name.get('acronymFr') or elt.get('acronymFr'),
        'label_en': name.get('nameEn'),
        'city': localisation.get('city'),
        'siren': sorted({x.replace(' ', '')[0:9] for x in sirets if len(x.replace(' ', '')) >= 9}),
        'uai': sorted(set(identifiers.get('uai', []))),
        'wikidata': sorted(set(identifiers.get('wikidata', []))),
        'main_category': (elt.get('category') or {}).get('id'),
        'main_category_label': (elt.get('category') or {}).get('usualNameFr'),
        'categories': {c.get('id') for c in elt.get('categories') or [] if isinstance(c, dict)},
        'iso3': localisation.get('iso3'),
        'rors': rors,
        'rnsrs': rnsrs,
    }


def cnrs_to_pivot(elt: dict) -> dict:
    """Fiche de l'annuaire CNRS (cnrs_annuaire.py). Les tutelles sont résolues plus tard."""
    code = elt['code'].strip().upper()
    location = elt.get('main_location') or {}
    rnsr = elt.get('rnsr').strip().upper() if isinstance(elt.get('rnsr'), str) and elt['rnsr'].strip() else None
    tutelles = [{'id': None, 'label': t, 'type': 'main'} for t in elt.get('tutelles_main') or []]
    tutelles += [{'id': None, 'label': t, 'type': 'secondary'} for t in elt.get('tutelles_secondary') or []]
    return {
        'source': 'cnrs',
        'id': code,
        'active': True,
        'code_type': re.match(r'^[A-Z]*', code).group(0),
        'label': elt.get('label'),
        'acronym': elt.get('acronym'),
        'codes': extract_codes([code]),
        'rnsr': rnsr,
        'city': location.get('city'),
        'postcode': location.get('postcode'),
        'country': location.get('country'),
        'address': ', '.join(x for x in (location.get('name'), location.get('street'), location.get('complement')) if x),
        'website': elt.get('website') or location.get('website'),
        'leaders': [{'role': l.get('role'), 'name': l.get('name')} for l in elt.get('leaders') or []],
        'institutes_main': elt.get('institutes_main') or [],
        'institutes_secondary': elt.get('institutes_secondary') or [],
        # tutelles principales et secondaires ; 'id' = clé paysage (ou ror:) une fois résolue
        'tutelles': tutelles,
        'last_modified': elt.get('last_modified'),
    }


def load_sources() -> dict:
    rnsr = {}
    for elt in fetch_dump('rnsr'):
        if isinstance(elt.get('id'), str):
            p = rnsr_to_pivot(elt)
            rnsr[p['id']] = p
    ror = {}
    for elt in fetch_dump('ror'):
        p = ror_to_pivot(elt)
        if p['id']:
            ror[p['id']] = p
    paysage = {}
    for elt in fetch_dump('paysage'):
        if isinstance(elt.get('id'), str):
            p = paysage_to_pivot(elt)
            paysage[p['id']] = p
    cnrs = {}
    try:
        for elt in fetch_dump('cnrs'):
            if isinstance(elt.get('code'), str) and elt['code'].strip():
                p = cnrs_to_pivot(elt)
                cnrs[p['id']] = p
    except Exception as e:
        logger.warning(f'cnrs : annuaire indisponible ({e}), diagnostic sans CNRS')
    geo_ref = load_geo_ref()
    for base in (rnsr, ror, cnrs):
        for x in base.values():
            x['place'] = resolve_place(x.get('city'), x.get('postcode'), x.get('country'), x.get('gps'), geo_ref)
    idref = load_idref_links()
    logger.debug(f'{len(rnsr)} RNSR, {len(ror)} ROR, {len(cnrs)} CNRS, {len(paysage)} paysage, '
                 f'idref={"indisponible" if idref is None else len(idref)}')
    return {'rnsr': rnsr, 'ror': ror, 'paysage': paysage, 'cnrs': cnrs, 'idref': idref}


def resolve_cnrs_tutelles(cnrs: dict, rnsr: dict, paysage: dict) -> list:
    """
    Libellé de tutelle CNRS -> clé paysage (ou ror:), en place dans cnrs[*]['tutelles'].
    1. libellé normalisé identique à un seul libellé paysage ou de tutelle RNSR
    2. sinon co-occurrence : sur les fiches CNRS dont le RNSR est connu, Jaccard entre les
       unités portant le libellé et celles dont le RNSR (tutelles + participants) porte
       la clé ; la meilleure clé est retenue si Jaccard >= seuil et sans ex aequo
    Renvoie la table de correspondance (une ligne par libellé) pour relecture.
    """
    label_units, key_units = defaultdict(set), defaultdict(set)
    for cu in cnrs.values():
        unit = rnsr.get(cu['rnsr'])
        if not unit:
            continue
        for t in cu['tutelles']:
            label_units[t['label']].add(cu['id'])
        for t in unit['tutelles'] + unit['participants']:
            key_units[tutelle_key_of_rnsr(t['id'], paysage)].add(cu['id'])
    name_index = defaultdict(set)
    for p in paysage.values():
        name_index[normalize_text(p['label'])].add(p['id'])
    for unit in rnsr.values():
        for t in unit['tutelles'] + unit['participants']:
            name_index[normalize_text(t['label'])].add(tutelle_key_of_rnsr(t['id'], paysage))
    name_index.pop('', None)

    all_labels = Counter(t['label'] for cu in cnrs.values() for t in cu['tutelles'])
    mapping, rows = {}, []
    for label, nb in all_labels.most_common():
        units = label_units.get(label, set())
        scores = []
        for key, k_units in key_units.items():
            inter = len(units & k_units)
            if inter:
                scores.append((inter / len(units | k_units), inter, key))
        scores.sort(reverse=True)
        best = scores[0] if scores else None
        second = scores[1][0] if len(scores) > 1 else 0
        key, method = None, None
        if len(name_index.get(normalize_text(label), [])) == 1:
            key, method = next(iter(name_index[normalize_text(label)])), 'label'
        elif best and best[0] >= CNRS_TUTELLE_MIN_JACCARD and best[0] > second:
            key, method = best[2], 'cooccurrence'
        elif best and best[0] >= CNRS_TUTELLE_MIN_JACCARD:
            method = 'ambiguous'
        mapping[label] = key
        rows.append({
            'cnrs_label': label, 'nb_cnrs_units': nb, 'nb_cnrs_units_with_rnsr': len(units),
            'key': key, 'key_label': tutelle_label(key, {'ror': {}, 'paysage': paysage}) if key else None,
            'method': method,
            'best_key': best[2] if best else None,
            'best_key_label': tutelle_label(best[2], {'ror': {}, 'paysage': paysage}) if best else None,
            'best_jaccard': round(best[0], 2) if best else None,
            'best_nb_common': best[1] if best else None,
            'second_jaccard': round(second, 2),
        })
    for cu in cnrs.values():
        for t in cu['tutelles']:
            t['id'] = mapping.get(t['label'])
    logger.debug(f"cnrs : {sum(1 for v in mapping.values() if v)} / {len(mapping)} libellés de tutelle rapprochés")
    return rows


# ---------------------------------------------------------------------------
# Étape 2 : périmètre ROR
# ---------------------------------------------------------------------------

def build_paysage_ror_index(paysage: dict) -> dict:
    """ROR -> ids paysage."""
    index = defaultdict(set)
    for p in paysage.values():
        for ror in p['rors']:
            index[ror].add(p['id'])
    return index


def tutelle_to_rors(tutelle_id: str, paysage: dict) -> list:
    if tutelle_id in paysage:
        return paysage[tutelle_id]['rors']
    ror = normalize_ror(tutelle_id)
    return [ror] if ror else []


def build_esr_etabs(rnsr: dict, paysage: dict) -> dict:
    """
    Établissements de l'ESR, indexés par ROR : {ror: {'paysage': [...], 'origins': [...]}}.
    Union des tutelles des RNSR actifs (hors entreprises) et des structures paysage
    actives, françaises, d'une catégorie ESR.
    """
    esr_paysage = defaultdict(set)
    for unit in rnsr.values():
        if not unit['active']:
            continue
        for t in unit['tutelles']:
            p = paysage.get(t['id'])
            if p and p['main_category'] in NON_ESR_MAIN_CATEGORIES:
                continue
            esr_paysage[t['id']].add('rnsr_tutelle')
    for p in paysage.values():
        if p['active'] and p['iso3'] in (None, 'FRA') and p['categories'] & ESR_PAYSAGE_CATEGORIES:
            esr_paysage[p['id']].add('paysage_category')
    esr = {}
    for pid, origins in esr_paysage.items():
        for ror in tutelle_to_rors(pid, paysage):
            entry = esr.setdefault(ror, {'paysage': set(), 'origins': set()})
            if pid in paysage:
                entry['paysage'].add(pid)
            entry['origins'] |= origins
    return esr


def ror_ancestors(ror_id: str, ror: dict, max_depth: int = MAX_PARENT_DEPTH) -> dict:
    """Ancêtres ROR (parents, grands-parents...) -> profondeur minimale."""
    ancestors = {}
    frontier = [ror_id]
    for depth in range(1, max_depth + 1):
        next_frontier = []
        for current in frontier:
            for parent in (ror.get(current) or {}).get('parents', []):
                if parent not in ancestors and parent != ror_id:
                    ancestors[parent] = depth
                    next_frontier.append(parent)
        frontier = next_frontier
    return ancestors


def build_ror_perimeter(ror: dict, esr: dict) -> dict:
    """{ror: {'depth': 1|2, 'esr_ancestors': [...]}} pour les ROR de niveau labo du périmètre."""
    esr_fr = {x for x in esr if (ror.get(x) or {}).get('country') == 'FR'}
    perimeter = {}
    for ror_id, r in ror.items():
        if not r['active'] or ror_id in esr:
            continue
        if not set(r['types']) & ROR_LAB_TYPES:
            continue
        ancestors = ror_ancestors(ror_id, ror)
        esr_ancestors = {a: d for a, d in ancestors.items() if a in esr_fr}
        if esr_ancestors:
            perimeter[ror_id] = {'depth': min(esr_ancestors.values()),
                                 'esr_ancestors': sorted(esr_ancestors)}
    return perimeter


# ---------------------------------------------------------------------------
# Étape 3 : liens
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def ror_label_variants(r: dict) -> list:
    """Noms ROR, avec et sans le préfixe « SIGLE: » (équipes-projets Inria notamment)."""
    out = []
    for n in r['names']:
        out.append(n)
        m = re.match(r'^\s*[^:]{1,30}\s*[:\-–]\s+(.+)$', n)
        if m:
            out.append(m.group(1))
    return out


def best_label_similarity(label, r: dict) -> float:
    if not label:
        return 0
    return max((similarity(label, n) for n in ror_label_variants(r)), default=0)


def shares_tutelle(unit: dict, r: dict, ror: dict, paysage: dict) -> bool:
    unit_tutelle_rors = {x for t in unit['tutelles'] for x in tutelle_to_rors(t['id'], paysage)}
    return bool(unit_tutelle_rors & set(ror_ancestors(r['id'], ror)))


def confirm_code_link(unit: dict, r: dict, ror: dict, paysage: dict) -> list:
    """
    Raisons qui confirment un lien par code d'unité : acronym, label, tutelle, city
    (même commune ou même unité urbaine).
    """
    reasons = []
    acronym = normalize_text(unit['acronym'])
    ror_acronyms = {normalize_text(a) for a in r['acronyms']} | {normalize_text(n) for n in r['names']}
    if acronym and acronym in ror_acronyms:
        reasons.append('acronym')
    if shares_tutelle(unit, r, ror, paysage):
        reasons.append('tutelle')
    if best_label_similarity(unit['label'], r) >= LABEL_SIMILARITY_CONFIRM:
        reasons.append('label')
    if records_place_relation(unit, r) in CITY_OK:
        reasons.append('city')
    return reasons


def code_link_decision(reasons: list) -> Optional[str]:
    """None = lien accepté, sinon motif de rejet."""
    if 'acronym' in reasons or 'label' in reasons:
        return None
    # une tutelle commune seule est trop faible (une université a des dizaines d'unités) ;
    # tutelle + même ville suffit
    if 'tutelle' in reasons and 'city' in reasons:
        return None
    return 'code_tutelle_only' if 'tutelle' in reasons else 'code_unconfirmed'


def link_weak(sources: dict, perimeter: dict, edges: list, add, rejected: list, weak: list) -> None:
    """
    Liens faibles, appliqués après tous les autres, seulement entre une fiche RNSR actif (ou
    CNRS) qui n'a pas encore de ROR et une fiche ROR du périmètre qui n'a ni RNSR ni CNRS,
    et seulement si le rapprochement est unique des deux côtés (sinon acronym_ambiguous /
    weak_ambiguous). Deux règles :
    - code : code d'unité commun + tutelle commune + même ville (liste `weak`). Sans cette
      précaution, un code repris par l'unité suivante (renouvellement) crée des conflits ;
    - acronym : même sigle + tutelle commune + même ville (commune ou unité urbaine) +
      libellés proches. Si la ville manque sur une des fiches, libellés très proches exigés.
    """
    rnsr, ror, cnrs, paysage = sources['rnsr'], sources['ror'], sources['cnrs'], sources['paysage']
    uf = UnionFind()
    for e in edges:
        uf.union(e['a'], e['b'])
    kinds_by_root = defaultdict(set)
    for e in edges:
        for node in (e['a'], e['b']):
            kinds_by_root[uf.find(node)].add(node.split(':')[0])

    def kinds(node):
        return kinds_by_root.get(uf.find(node), set()) if node in uf.parent else set()

    def unit_free(node):
        kind, unit_id = node.split(':', 1)
        if kind == 'rnsr':
            return rnsr[unit_id]['active'] and 'ror' not in kinds(node)
        return not kinds(node) & {'ror', 'rnsr'}

    def ror_free(node):
        return node[4:] in perimeter and not kinds(node) & {'rnsr', 'cnrs'}

    candidates = defaultdict(list)
    for row in weak:
        if unit_free(row['a']) and ror_free(row['b']):
            candidates[row['a']].append(row)
        else:
            rejected.append(dict(row, reason='code_weak_already_linked'))

    ror_index = defaultdict(list)
    for ror_id in perimeter:
        if not ror_free(f'ror:{ror_id}'):
            continue
        r = ror[ror_id]
        acronyms = {normalize_text(a) for a in r['acronyms']}
        acronyms |= {normalize_text(n.split(':')[0]) for n in r['names'] if ':' in n}
        for a in acronyms:
            if len(a) >= 2:
                ror_index[a].append(ror_id)
    units = [('rnsr', u) for u in rnsr.values() if unit_free(f"rnsr:{u['id']}")]
    units += [('cnrs', u) for u in cnrs.values() if unit_free(f"cnrs:{u['id']}")]
    for kind, unit in units:
        acronym = normalize_text(unit['acronym'])
        for ror_id in ror_index.get(acronym, []) if len(acronym) >= 2 else []:
            node = f"{kind}:{unit['id']}"
            if any(row['b'] == f'ror:{ror_id}' for row in candidates.get(node, [])):
                continue
            r = ror[ror_id]
            city = records_place_relation(unit, r)
            sim = best_label_similarity(unit['label'], r)
            tutelle = shares_tutelle(unit, r, ror, paysage)
            row = {'a': node, 'b': f'ror:{ror_id}', 'source': 'acronym',
                   'detail': f"{unit['acronym']} (city={city}, tutelle={tutelle}, label={sim:.2f})"}
            if not tutelle:
                rejected.append(dict(row, reason='acronym_no_tutelle'))
            elif city == 'different':
                rejected.append(dict(row, reason='acronym_city_different'))
            elif city == 'unknown' and sim < LABEL_SIMILARITY_CLOSE:
                rejected.append(dict(row, reason='acronym_city_unknown'))
            elif sim < LABEL_SIMILARITY_ACRONYM:
                rejected.append(dict(row, reason='acronym_label'))
            else:
                candidates[node].append(row)

    claims = Counter(row['b'] for rows in candidates.values() for row in rows)
    for rows in candidates.values():
        for row in rows:
            if len(rows) > 1 or claims[row['b']] > 1:
                rejected.append(dict(row, reason=f"{'acronym' if row['source'] == 'acronym' else 'code_weak'}_ambiguous"))
            else:
                add(row['a'], row['b'], row['source'], row['detail'])


def build_links(sources: dict, esr: dict, perimeter: dict) -> tuple:
    """
    Arêtes du graphe d'identifiants. Noeuds : 'rnsr:X', 'ror:Y', 'paysage:Z', 'cnrs:CODE'.
    Renvoie (edges, rejected) ; chaque arête porte sa source et un détail.
    """
    rnsr, ror, paysage, idref = sources['rnsr'], sources['ror'], sources['paysage'], sources['idref']
    cnrs = sources['cnrs']
    edges, rejected = [], []

    def add(a, b, source, detail=None):
        # un lien vers un établissement ESR fusionnerait des dizaines de labos : rejeté
        for node in (a, b):
            if node.startswith('ror:') and node[4:] in esr:
                rejected.append({'a': a, 'b': b, 'source': source, 'reason': 'link_to_etab', 'detail': detail})
                return
        if a.startswith('rnsr:') and a[5:] not in rnsr:
            rejected.append({'a': a, 'b': b, 'source': source, 'reason': 'unknown_rnsr', 'detail': detail})
            return
        edges.append({'a': a, 'b': b, 'source': source, 'detail': detail})

    # IdRef (SPARQL), repli sur les ROR des externalIds de rnsr-v2
    if idref is not None:
        for rnsr_id, pids in idref.items():
            for pid in pids:
                ror_id = normalize_ror(pid.get('id')) if pid.get('type') == 'ror' else None
                if ror_id:
                    add(f'rnsr:{rnsr_id.strip().upper()}', f'ror:{ror_id}', 'idref')
    else:
        for unit in rnsr.values():
            for ror_id in unit['rors_from_dump']:
                add(f"rnsr:{unit['id']}", f'ror:{ror_id}', 'idref_via_rnsr_dump')

    # paysage : une fiche paysage portant rnsr et/ou ror
    esr_paysage = {pid for e in esr.values() for pid in e['paysage']}
    for p in paysage.values():
        if not p['rnsrs'] or not (p['rors'] or len(p['rnsrs']) > 1):
            continue
        if p['id'] in esr_paysage:
            rejected.append({'a': f"paysage:{p['id']}", 'b': '|'.join(p['rnsrs']), 'source': 'paysage',
                             'reason': 'paysage_etab_with_rnsr', 'detail': p['label']})
            continue
        for rnsr_id in p['rnsrs']:
            add(f'rnsr:{rnsr_id}', f"paysage:{p['id']}", 'paysage')
        for ror_id in p['rors']:
            add(f'ror:{ror_id}', f"paysage:{p['id']}", 'paysage')

    # annuaire CNRS : RNSR déclaré sur la fiche
    for cu in cnrs.values():
        if cu['rnsr']:
            add(f"rnsr:{cu['rnsr']}", f"cnrs:{cu['id']}", 'cnrs')

    # codes d'unité : ROR du périmètre -> RNSR (actifs et inactifs) et -> CNRS, avec confirmation
    weak = []
    for kind, base, source in (('rnsr', rnsr, 'code'), ('cnrs', cnrs, 'cnrs_code')):
        code_index = defaultdict(set)
        for unit in base.values():
            for code in unit['codes']:
                code_index[code].add(unit['id'])
        for ror_id in perimeter:
            r = ror[ror_id]
            for code in r['codes']:
                for unit_id in code_index.get(code, []):
                    reasons = confirm_code_link(base[unit_id], r, ror, paysage)
                    detail = f"{code[0]} {code[1]} ({'+'.join(reasons)}, city={records_place_relation(base[unit_id], r)})"
                    reason = code_link_decision(reasons)
                    if reason is None and 'acronym' not in reasons and 'label' not in reasons:
                        # tutelle + ville : lien faible, appliqué en dernier (voir link_weak)
                        weak.append({'a': f'{kind}:{unit_id}', 'b': f'ror:{ror_id}', 'source': source,
                                     'detail': detail})
                    elif reason is None:
                        add(f'{kind}:{unit_id}', f'ror:{ror_id}', source, detail)
                    else:
                        rejected.append({'a': f'{kind}:{unit_id}', 'b': f'ror:{ror_id}', 'source': source,
                                         'reason': reason, 'detail': detail})

    # codes d'unité : CNRS sans RNSR déclaré -> RNSR, avec confirmation par sigle ou libellé
    rnsr_code_index = defaultdict(set)
    for unit in rnsr.values():
        for code in unit['codes']:
            rnsr_code_index[code].add(unit['id'])
    for cu in cnrs.values():
        if cu['rnsr']:
            continue
        for code in cu['codes']:
            for rnsr_id in rnsr_code_index.get(code, []):
                unit = rnsr[rnsr_id]
                reasons = []
                if normalize_text(cu['acronym']) and normalize_text(cu['acronym']) == normalize_text(unit['acronym']):
                    reasons.append('acronym')
                if similarity(cu['label'] or '', unit['label'] or '') >= LABEL_SIMILARITY_CONFIRM:
                    reasons.append('label')
                # même code d'unité (même nomenclature) + même ville suffit
                if records_place_relation(cu, unit) in CITY_OK:
                    reasons.append('city')
                detail = f"{code[0]} {code[1]} (city={records_place_relation(cu, unit)})"
                if reasons:
                    add(f'rnsr:{rnsr_id}', f"cnrs:{cu['id']}", 'cnrs_code_rnsr', f"{detail} ({'+'.join(reasons)})")
                else:
                    rejected.append({'a': f'rnsr:{rnsr_id}', 'b': f"cnrs:{cu['id']}", 'source': 'cnrs_code_rnsr',
                                     'reason': 'code_unconfirmed', 'detail': detail})
    # liens faibles en dernier : code + tutelle + ville, et sigle + tutelle + ville
    link_weak(sources, perimeter, edges, add, rejected, weak)
    logger.debug(f'{len(edges)} liens, {len(rejected)} rejetés ({dict(Counter(e["source"] for e in edges))})')
    return edges, rejected


def build_clusters(edges: list, sources: dict, perimeter: dict) -> list:
    uf = UnionFind()
    for unit in sources['rnsr'].values():
        if unit['active']:
            uf.find(f"rnsr:{unit['id']}")
    for ror_id in perimeter:
        uf.find(f'ror:{ror_id}')
    for cnrs_id in sources['cnrs']:
        uf.find(f'cnrs:{cnrs_id}')
    for e in edges:
        uf.union(e['a'], e['b'])
    members = defaultdict(set)
    for node in list(uf.parent):
        members[uf.find(node)].add(node)
    edges_by_root = defaultdict(list)
    for e in edges:
        edges_by_root[uf.find(e['a'])].append(e)
    clusters = []
    for root, nodes in members.items():
        cluster = describe_cluster(nodes, edges_by_root[root], sources, perimeter)
        if cluster:
            clusters.append(cluster)
    return clusters


# ---------------------------------------------------------------------------
# Étape 4 : diagnostic
# ---------------------------------------------------------------------------

def describe_cluster(nodes: set, edges: list, sources: dict, perimeter: dict) -> Optional[dict]:
    rnsr, ror = sources['rnsr'], sources['ror']
    ids = defaultdict(list)
    for node in sorted(nodes):
        kind, value = node.split(':', 1)
        ids[kind].append(value)
    rnsr_active = [x for x in ids['rnsr'] if rnsr[x]['active']]
    rnsr_old = [x for x in ids['rnsr'] if not rnsr[x]['active']]
    ror_perimeter = [x for x in ids['ror'] if x in perimeter]
    ror_other_active = [x for x in ids['ror'] if x not in perimeter and (ror.get(x) or {}).get('active')]
    ror_inactive = [x for x in ids['ror'] if x in ror and not ror[x]['active']]
    ror_unknown = [x for x in ids['ror'] if x not in ror]
    cnrs_ids = ids['cnrs']
    if not rnsr_active and not ror_perimeter and not cnrs_ids:
        return None  # hors périmètre (unités fermées uniquement)
    active_rors = ror_perimeter + ror_other_active
    if len(rnsr_active) >= 2:
        category = 'conflict_multi_rnsr'
    elif len(active_rors) >= 2:
        category = 'conflict_multi_ror'
    elif len(cnrs_ids) >= 2:
        category = 'conflict_multi_cnrs'
    elif cnrs_ids and not rnsr_active:
        # fiche CNRS sans RNSR actif : avec ou sans ROR, avec ou sans RNSR fermé
        if ror_perimeter or ror_other_active:
            category = 'cnrs_ror_no_rnsr'
        elif rnsr_old:
            category = 'cnrs_rnsr_old'
        else:
            category = 'cnrs_only'
    elif rnsr_active:
        if ror_perimeter:
            category = 'rnsr_ror'
        elif ror_other_active:
            category = 'rnsr_ror_out_of_perimeter'
        elif ror_inactive:
            category = 'rnsr_ror_inactive'
        else:
            category = 'rnsr_only'
    elif rnsr_old:
        category = 'ror_rnsr_old'
    else:
        category = 'ror_only'
    # clé stable de la structure : RNSR actif, sinon code CNRS, sinon ROR, sinon RNSR fermé
    key_source = (rnsr_active and f'rnsr:{rnsr_active[0]}') or (cnrs_ids and f'cnrs:{cnrs_ids[0]}') \
        or (active_rors and f'ror:{active_rors[0]}') or f'rnsr:{rnsr_old[0]}'
    return {
        'key': key_source,
        'category': category,
        'rnsr_active': rnsr_active,
        'rnsr_old': rnsr_old,
        'ror_perimeter': ror_perimeter,
        'ror_other_active': ror_other_active,
        'ror_inactive': ror_inactive,
        'ror_unknown': ror_unknown,
        'paysage': ids['paysage'],
        'cnrs': cnrs_ids,
        'sources_present': '+'.join(k for k, present in (('rnsr', rnsr_active), ('ror', active_rors),
                                                         ('cnrs', cnrs_ids)) if present),
        'link_sources': sorted({e['source'] for e in edges}),
        'edges': edges,
    }


def compare_tutelles(unit: dict, r: dict, sources: dict) -> dict:
    """Tutelles RNSR (via paysage -> ROR) vs parents ROR (avec leurs propres parents)."""
    ror, paysage = sources['ror'], sources['paysage']
    rnsr_rors, unmapped = set(), []
    for t in unit['tutelles']:
        mapped = tutelle_to_rors(t['id'], paysage)
        rnsr_rors |= set(mapped)
        if not mapped:
            unmapped.append(t['id'])
    parents = set(r['parents'])
    ancestors = set(ror_ancestors(r['id'], ror))
    if not rnsr_rors and not parents:
        status = 'missing_both'
    elif not rnsr_rors:
        status = 'missing_rnsr'
    elif not parents:
        status = 'missing_ror'
    else:
        # chaque tutelle RNSR apparaît dans l'ascendance ROR ?
        rnsr_covered = rnsr_rors <= ancestors
        # chaque parent ROR est une tutelle RNSR, ou rattaché à l'une d'elles ?
        ror_covered = all(p in rnsr_rors or set((ror.get(p) or {}).get('parents', [])) & rnsr_rors for p in parents)
        if rnsr_covered and ror_covered:
            status = 'same'
        elif rnsr_covered:
            status = 'ror_more'
        elif ror_covered:
            status = 'rnsr_more'
        elif rnsr_rors & ancestors:
            status = 'overlap'
        else:
            status = 'disjoint'
    return {
        'status': status,
        'rnsr_tutelles': sorted(rnsr_rors),
        'rnsr_tutelles_unmapped': unmapped,
        'ror_parents': sorted(parents),
        'only_rnsr': sorted(rnsr_rors - ancestors),
        'only_ror': sorted(p for p in parents if p not in rnsr_rors
                           and not set((ror.get(p) or {}).get('parents', [])) & rnsr_rors),
    }


def compare_referent(unit: dict, r: dict, sources: dict) -> str:
    """La tutelle référente RNSR figure-t-elle dans l'ascendance ROR ?"""
    if not unit['referent_known']:
        return 'unknown'
    if not unit['referents']:
        return 'none_declared'
    if len(unit['referents']) > 1:
        return 'multiple'
    referent_rors = set(tutelle_to_rors(unit['referents'][0], sources['paysage']))
    if not referent_rors:
        return 'unmapped'
    return 'in_ror' if referent_rors & set(ror_ancestors(r['id'], sources['ror'])) else 'not_in_ror'


def compare_values(a, b, same, close=None, names=('rnsr', 'ror')) -> str:
    if not a and not b:
        return 'missing_both'
    if not a:
        return f'missing_{names[0]}'
    if not b:
        return f'missing_{names[1]}'
    if same(a, b):
        return 'same'
    if close and close(a, b):
        return 'close'
    return 'different'


def compare_pair(unit: dict, r: dict, sources: dict) -> dict:
    """Désaccords bruts champ par champ entre une unité RNSR et sa fiche ROR."""
    ror_labels = [n for n in r['names'] if n not in r['acronyms']]
    best_label = max(ror_labels, key=lambda n: similarity(unit['label'] or '', n), default=None)
    label_score = similarity(unit['label'] or '', best_label or '')

    def same_url(a, b):
        return normalize_url(a)[1] == normalize_url(b)[1]

    def same_domain(a, b):
        return normalize_url(a)[0] == normalize_url(b)[0]

    tutelles = compare_tutelles(unit, r, sources)
    return {
        'label': compare_values(unit['label'], best_label,
                                lambda a, b: normalize_text(a) == normalize_text(b),
                                lambda a, b: label_score >= LABEL_SIMILARITY_CLOSE),
        'label_score': round(label_score, 2),
        'acronym': compare_values(unit['acronym'], r['acronyms'],
                                  lambda a, b: normalize_text(a) in {normalize_text(x) for x in b}),
        'website': compare_values(unit['website'], r['website'], same_url, same_domain),
        'city': compare_values(unit['city'], r['city'], lambda a, b: records_place_relation(unit, r) == 'same_city',
                               lambda a, b: records_place_relation(unit, r) == 'same_uu'),
        'start_year': compare_values(unit['start_year'], r['start_year'], lambda a, b: a == b),
        'tutelles': tutelles['status'],
        'referent': compare_referent(unit, r, sources),
        'label_en': 'present' if r['label_en'] else 'missing_ror',
        'gps': 'present' if r['gps'] else 'missing_ror',
        '_details': {
            'rnsr_label': unit['label'], 'ror_label': best_label,
            'rnsr_acronym': unit['acronym'], 'ror_acronyms': r['acronyms'],
            'rnsr_website': unit['website'], 'ror_website': r['website'],
            'rnsr_city': unit['city'], 'ror_city': r['city'],
            'rnsr_start_year': unit['start_year'], 'ror_start_year': r['start_year'],
            **{f'tutelles_{k}': v for k, v in tutelles.items() if k != 'status'},
            'rnsr_referent': unit['referents'],
            'rnsr_referent_labels': referent_labels(unit),
        },
    }


def compare_rnsr_cnrs(unit: dict, cu: dict, sources: dict) -> dict:
    """Désaccords bruts champ par champ entre une unité RNSR et sa fiche de l'annuaire CNRS."""
    paysage = sources['paysage']
    names = ('rnsr', 'cnrs')
    rnsr_keys = {tutelle_key_of_rnsr(t['id'], paysage) for t in unit['tutelles']}
    rnsr_participants = {tutelle_key_of_rnsr(t['id'], paysage) for t in unit['participants']}
    cnrs_main = {t['id'] for t in cu['tutelles'] if t['type'] == 'main' and t['id']}
    cnrs_secondary = {t['id'] for t in cu['tutelles'] if t['type'] == 'secondary' and t['id']}
    unmapped = [t['label'] for t in cu['tutelles'] if not t['id']]
    if not rnsr_keys and not cnrs_main:
        tutelles = 'missing_both'
    elif not cnrs_main:
        tutelles = 'missing_cnrs'
    elif not rnsr_keys:
        tutelles = 'missing_rnsr'
    elif rnsr_keys == cnrs_main:
        tutelles = 'same'
    elif cnrs_main < rnsr_keys:
        tutelles = 'rnsr_more'
    elif rnsr_keys < cnrs_main:
        tutelles = 'cnrs_more'
    elif rnsr_keys & cnrs_main:
        tutelles = 'overlap'
    else:
        tutelles = 'disjoint'
    if not cnrs_secondary:
        secondary = 'none'
    elif cnrs_secondary <= rnsr_participants:
        secondary = 'rnsr_participant'
    elif cnrs_secondary <= rnsr_participants | rnsr_keys:
        secondary = 'rnsr_tutelle_or_participant'
    else:
        secondary = 'absent_from_rnsr'
    label_score = similarity(unit['label'] or '', cu['label'] or '')
    return {
        'code': compare_values(unit['codes'], cu['codes'], lambda a, b: bool(a & b), names=names),
        'label': compare_values(unit['label'], cu['label'],
                                lambda a, b: normalize_text(a) == normalize_text(b),
                                lambda a, b: label_score >= LABEL_SIMILARITY_CLOSE, names=names),
        'label_score': round(label_score, 2),
        'acronym': compare_values(unit['acronym'], cu['acronym'],
                                  lambda a, b: normalize_text(a) == normalize_text(b), names=names),
        'website': compare_values(unit['website'], cu['website'],
                                  lambda a, b: normalize_url(a)[1] == normalize_url(b)[1],
                                  lambda a, b: normalize_url(a)[0] == normalize_url(b)[0], names=names),
        'city': compare_values(unit['city'], cu['city'], lambda a, b: records_place_relation(unit, cu) == 'same_city',
                               lambda a, b: records_place_relation(unit, cu) == 'same_uu', names=names),
        'postcode': compare_values(unit['postcode'], cu['postcode'], lambda a, b: str(a)[0:2] == str(b)[0:2],
                                   names=names),
        'tutelles': tutelles,
        'cnrs_secondary_tutelles': secondary,
        '_details': {
            'rnsr_label': unit['label'], 'cnrs_label': cu['label'],
            'rnsr_acronym': unit['acronym'], 'cnrs_acronym': cu['acronym'],
            'rnsr_codes': ' '.join(f'{f} {n}' for f, n in sorted(unit['codes'])), 'cnrs_code': cu['id'],
            'rnsr_website': unit['website'], 'cnrs_website': cu['website'],
            'rnsr_city': unit['city'], 'cnrs_city': cu['city'],
            'rnsr_tutelles': sorted(rnsr_keys), 'cnrs_tutelles_main': sorted(cnrs_main),
            'only_rnsr': sorted(rnsr_keys - cnrs_main), 'only_cnrs': sorted(cnrs_main - rnsr_keys),
            'only_rnsr_labels': [tutelle_label(k, sources) for k in sorted(rnsr_keys - cnrs_main)],
            'only_cnrs_labels': [tutelle_label(k, sources) for k in sorted(cnrs_main - rnsr_keys)],
            'cnrs_tutelles_secondary': sorted(cnrs_secondary),
            'cnrs_tutelles_unmapped': unmapped,
            'rnsr_referent_labels': referent_labels(unit),
            'cnrs_institute': cu['institutes_main'],
        },
    }


def idref_paysage_contradictions(edges: list, sources: dict) -> list:
    """RNSR actifs pour lesquels IdRef et paysage donnent des ROR disjoints."""
    idref_rors = defaultdict(set)
    paysage_of_rnsr = defaultdict(set)
    rors_of_paysage = defaultdict(set)
    for e in edges:
        if e['source'].startswith('idref'):
            idref_rors[e['a'][5:]].add(e['b'][4:])
        elif e['source'] == 'paysage':
            if e['a'].startswith('rnsr:'):
                paysage_of_rnsr[e['a'][5:]].add(e['b'][8:])
            else:
                rors_of_paysage[e['b'][8:]].add(e['a'][4:])
    rows = []
    for rnsr_id, rors in idref_rors.items():
        unit = sources['rnsr'].get(rnsr_id)
        if not unit or not unit['active']:
            continue
        p_rors = {r for p in paysage_of_rnsr.get(rnsr_id, []) for r in rors_of_paysage.get(p, [])}
        if p_rors and not rors & p_rors:
            rows.append({'rnsr': rnsr_id, 'label': unit['label'], 'idref_rors': sorted(rors),
                         'paysage_rors': sorted(p_rors), 'paysage': sorted(paysage_of_rnsr[rnsr_id])})
    return rows


def labels_of(ids: list, base: dict) -> list:
    return [(base.get(x) or {}).get('label') for x in ids]


def referent_labels(unit: dict) -> list:
    return [t['label'] or t['id'] for t in unit['tutelles'] if t['referent'] is True]


# ---------------------------------------------------------------------------
# Vue par tutelle
# ---------------------------------------------------------------------------

def tutelle_key_of_ror(ror_id: str, esr: dict, paysage_ror_index: dict) -> str:
    """Une tutelle est identifiée par son id paysage quand il existe, sinon 'ror:<id>'."""
    candidates = sorted((esr.get(ror_id) or {}).get('paysage') or paysage_ror_index.get(ror_id) or [])
    return candidates[0] if candidates else f'ror:{ror_id}'


def tutelle_key_of_rnsr(tutelle_id: str, paysage: dict) -> str:
    if tutelle_id in paysage:
        return tutelle_id
    ror = normalize_ror(tutelle_id)
    return f'ror:{ror}' if ror else tutelle_id


def tutelle_label(key: str, sources: dict) -> Optional[str]:
    if key.startswith('cnrs_label:'):
        return key[len('cnrs_label:'):]
    if key.startswith('ror:'):
        return (sources['ror'].get(key[4:]) or {}).get('label')
    return (sources['paysage'].get(key) or {}).get('label')


def build_tutelle_view(clusters: list, sources: dict, esr: dict, paysage_ror_index: dict) -> tuple:
    """
    Une ligne par (tutelle, cluster) : la tutelle est-elle vue par le RNSR, par ROR, par le CNRS ?
    - seen_by : sources qui la donnent (rnsr, ror, cnrs = tutelle principale CNRS)
    - agreement (RNSR vs ROR) = both : RNSR et ROR la donnent
      rnsr_only_no_ror / ror_only_no_rnsr : l'autre source n'a pas de fiche pour l'unité
      rnsr_only_ror_disagrees / ror_only_rnsr_disagrees : l'autre source a une fiche,
      mais sans cette tutelle ; neither : seul le CNRS la donne
    - cnrs_view : main / secondary / absent (fiche CNRS sans cette tutelle) / no_cnrs_unit
    Les libellés CNRS non rapprochés de paysage sont gardés sous la clé 'cnrs_label:<libellé>'.
    Renvoie (lignes détaillées, une ligne de synthèse par tutelle).
    """
    rnsr, ror, paysage = sources['rnsr'], sources['ror'], sources['paysage']
    rows = []
    for c in clusters:
        units = [rnsr[x] for x in c['rnsr_active']]
        rors = [ror[x] for x in c['ror_perimeter'] + c['ror_other_active']]
        cnrs_units = [sources['cnrs'][x] for x in c['cnrs']]
        from_cnrs = {}
        for cu in cnrs_units:
            for t in cu['tutelles']:
                key = t['id'] or f"cnrs_label:{t['label']}"
                if from_cnrs.get(key) != 'main':
                    from_cnrs[key] = t['type']
        from_rnsr = {}
        for unit in units:
            for t in unit['tutelles']:
                key = tutelle_key_of_rnsr(t['id'], paysage)
                previous = from_rnsr.get(key)
                # True l'emporte sur False, qui l'emporte sur None (inconnu)
                if previous is None or t['referent'] is True:
                    from_rnsr[key] = t['referent']
        from_ror = {}
        for r in rors:
            for ancestor, depth in ror_ancestors(r['id'], ror).items():
                if ancestor in esr:
                    key = tutelle_key_of_ror(ancestor, esr, paysage_ror_index)
                    from_ror[key] = min(depth, from_ror.get(key, depth))
        for key in sorted(set(from_rnsr) | set(from_ror) | set(from_cnrs)):
            in_rnsr, in_ror = key in from_rnsr, key in from_ror
            in_cnrs = from_cnrs.get(key) == 'main'
            if in_rnsr and in_ror:
                agreement = 'both'
            elif in_rnsr:
                agreement = 'rnsr_only_ror_disagrees' if rors else 'rnsr_only_no_ror'
            elif in_ror:
                agreement = 'ror_only_rnsr_disagrees' if units else 'ror_only_no_rnsr'
            else:
                agreement = 'neither'
            if not cnrs_units:
                cnrs_view = 'no_cnrs_unit'
            else:
                cnrs_view = from_cnrs.get(key) or 'absent'
            seen_by = '+'.join(k for k, v in (('rnsr', in_rnsr), ('ror', in_ror), ('cnrs', in_cnrs)) if v)
            rows.append({
                'tutelle': key,
                'tutelle_label': tutelle_label(key, sources),
                'cluster': c['cluster'],
                'structure_key': c['key'],
                'category': c['category'],
                'agreement': agreement,
                'rnsr': [u['id'] for u in units],
                'rnsr_label': [u['label'] for u in units],
                'rnsr_level': [u['level'] for u in units],
                'in_rnsr': in_rnsr,
                'referent': from_rnsr.get(key) if in_rnsr else None,
                'unit_referent_labels': sorted({l for u in units for l in referent_labels(u)}),
                'ror': [r['id'] for r in rors],
                'ror_label': [r['label'] for r in rors],
                'in_ror': in_ror,
                'ror_depth': from_ror.get(key),
                'cnrs': [cu['id'] for cu in cnrs_units],
                'cnrs_label': [cu['label'] for cu in cnrs_units],
                'in_cnrs': in_cnrs,
                'cnrs_view': cnrs_view,
                'seen_by': seen_by or 'cnrs_secondary_only',
            })
    summary = {}
    for row in rows:
        s = summary.setdefault(row['tutelle'], Counter())
        s['nb_units'] += 1
        s[row['agreement']] += 1
        s['in_rnsr'] += row['in_rnsr']
        s['in_ror'] += row['in_ror']
        s['in_cnrs'] += row['in_cnrs']
        s[f"cnrs_{row['cnrs_view']}"] += 1
        s['referent'] += row['referent'] is True
    agreements = ['both', 'rnsr_only_no_ror', 'rnsr_only_ror_disagrees', 'ror_only_no_rnsr', 'ror_only_rnsr_disagrees',
                  'neither']
    cnrs_views = ['cnrs_main', 'cnrs_secondary', 'cnrs_absent', 'cnrs_no_cnrs_unit']
    summary_rows = []
    for key, s in summary.items():
        summary_rows.append({
            'tutelle': key,
            'tutelle_label': tutelle_label(key, sources),
            'rors': [key[4:]] if key.startswith('ror:') else (paysage.get(key) or {}).get('rors', []),
            'esr_origins': sorted({o for r in ([key[4:]] if key.startswith('ror:') else (paysage.get(key) or {}).get('rors', []))
                                   for o in (esr.get(r) or {}).get('origins', [])}),
            'nb_units': s['nb_units'],
            'nb_in_rnsr': s['in_rnsr'],
            'nb_referent': s['referent'],
            'nb_in_ror': s['in_ror'],
            'nb_in_cnrs': s['in_cnrs'],
            **{a: s[a] for a in agreements},
            'pct_both': round(100 * s['both'] / s['nb_units'], 1),
            **{v: s[v] for v in cnrs_views},
        })
    summary_rows.sort(key=lambda x: -x['nb_units'])
    return rows, summary_rows


def slugify(text, max_len: int = 50) -> str:
    return normalize_text(text).replace(' ', '_')[:max_len] or 'sans_nom'


def write_tutelle_files(rows: list, summary_rows: list, output_dir: str) -> None:
    """tutelles_summary.csv, tutelles_units.csv, et un fichier par tutelle dans tutelles/."""
    write_csv(summary_rows, os.path.join(output_dir, 'tutelles_summary.csv'))
    write_csv(rows, os.path.join(output_dir, 'tutelles_units.csv'))
    tutelle_dir = os.path.join(output_dir, 'tutelles')
    os.makedirs(tutelle_dir, exist_ok=True)
    for f in os.listdir(tutelle_dir):
        if f.endswith('.csv'):
            os.remove(os.path.join(tutelle_dir, f))
    by_tutelle = defaultdict(list)
    for row in rows:
        by_tutelle[row['tutelle']].append(row)
    for key, tutelle_rows in by_tutelle.items():
        if key.startswith('cnrs_label:'):
            name = f"cnrs_label_{slugify(tutelle_rows[0]['tutelle_label'])}.csv"
        else:
            name = f"{key.replace(':', '_')}_{slugify(tutelle_rows[0]['tutelle_label'])}.csv"
        write_csv(tutelle_rows, os.path.join(tutelle_dir, name))


def referent_anomalies(rnsr: dict) -> list:
    """RNSR actifs dont le champ référent est renseigné mais sans référent, ou avec plusieurs."""
    rows = []
    for unit in rnsr.values():
        if unit['active'] and unit['referent_known'] and len(unit['referents']) != 1:
            rows.append({'rnsr': unit['id'], 'label': unit['label'], 'level': unit['level'],
                         'nb_referents': len(unit['referents']), 'referents': referent_labels(unit),
                         'tutelles': [t['label'] or t['id'] for t in unit['tutelles']]})
    return rows


# ---------------------------------------------------------------------------
# Vue consolidée : une entrée par structure, les valeurs de chaque source
# (pour alimenter la vue de curation RNeST)
# ---------------------------------------------------------------------------

SOURCES_ORDER = ('rnsr', 'cnrs', 'ror')


def format_codes(codes) -> list:
    return [f'{f} {n}' for f, n in sorted(codes)]


def record_values(kind: str, x: dict) -> dict:
    """Valeurs d'une fiche source, aux noms de champs RNeST."""
    if kind == 'rnsr':
        return {
            'acronyme': [x['acronym']], 'libelle_long_fr': [x['label']], 'libelle_en': [],
            'code_unite': format_codes(x['codes']) or x['label_numero'], 'type': [x['level']],
            'site_web': [x['website']], 'localite': [x['city']], 'code_postal': [x['postcode']],
            'pays': [x['country']], 'adresse': [x['address']], 'gps': [],
            'annee_debut': [x['start_year']],
        }
    if kind == 'cnrs':
        return {
            'acronyme': [x['acronym']], 'libelle_long_fr': [x['label']], 'libelle_en': [],
            'code_unite': format_codes(x['codes']) or [x['id']], 'type': [x['code_type']],
            'site_web': [x['website']], 'localite': [x['city']], 'code_postal': [x['postcode']],
            'pays': [x['country']], 'adresse': [x['address']], 'gps': [],
            'annee_debut': [],
        }
    return {
        'acronyme': x['acronyms'], 'libelle_long_fr': [x['label_fr'] or x['label']], 'libelle_en': [x['label_en']],
        'code_unite': format_codes(x['codes']), 'type': ['+'.join(x['types'])],
        'site_web': [x['website']], 'localite': [x['city']], 'code_postal': [],
        'pays': [x['country']], 'adresse': [],
        'gps': [f"{x['gps']['lat']},{x['gps']['lon']}"] if x['gps'] else [],
        'annee_debut': [x['start_year']],
    }


FIELD_NORMALIZERS = {
    'acronyme': normalize_text,
    'libelle_long_fr': normalize_text,
    'libelle_en': normalize_text,
    'code_unite': lambda v: re.sub(r'\s+', ' ', str(v).upper()).strip(),
    'site_web': lambda v: normalize_url(v)[1],
    'localite': normalize_city,
    'code_postal': lambda v: str(v).strip()[0:2],  # département : les codes CEDEX diffèrent
    'annee_debut': lambda v: str(v),
}
# champs propres à une source ou de nomenclatures différentes : valeurs sans statut
FIELDS_WITHOUT_STATUS = {'type', 'pays', 'adresse', 'gps', 'libelle_en'}


def field_status(values: dict, field: str) -> str:
    """
    same : toutes les sources présentes partagent une valeur ; partial : deux sources sur
    trois ; close : libellés proches ; different ; only_<source> ; missing.
    """
    present = {src: v for src, v in values.items() if v}
    if not present:
        return 'missing'
    if len(present) == 1:
        return f'only_{next(iter(present))}'
    norm = FIELD_NORMALIZERS[field]
    sets = {src: {norm(x) for x in v} - {'', None} for src, v in present.items()}
    if set.intersection(*sets.values()):
        return 'same'
    srcs = list(sets)
    if any(sets[a] & sets[b] for i, a in enumerate(srcs) for b in srcs[i + 1:]):
        return 'partial'
    if field == 'libelle_long_fr' and any(similarity(x, y) >= LABEL_SIMILARITY_CLOSE
                                          for i, a in enumerate(srcs) for b in srcs[i + 1:]
                                          for x in present[a] for y in present[b]):
        return 'close'
    if field == 'site_web' and any(normalize_url(x)[0] == normalize_url(y)[0]
                                   for i, a in enumerate(srcs) for b in srcs[i + 1:]
                                   for x in present[a] for y in present[b]):
        return 'close'
    return 'different'


def build_structures_view(clusters: list, sources: dict, tutelle_rows: list) -> list:
    """Une entrée par cluster : présence par source, identifiants, valeurs par champ et par source,
    établissements (vus par quelles sources), référent, responsables, points à curer."""
    rnsr, ror, cnrs, paysage = sources['rnsr'], sources['ror'], sources['cnrs'], sources['paysage']
    cnrs_key = next((t['id'] for cu in cnrs.values() for t in cu['tutelles'] if t['label'] == 'CNRS' and t['id']), None)
    etabs_by_cluster = defaultdict(list)
    for row in tutelle_rows:
        etabs_by_cluster[row['cluster']].append(row)
    entries = []
    for c in clusters:
        records = {
            'rnsr': [rnsr[x] for x in c['rnsr_active']],
            'cnrs': [cnrs[x] for x in c['cnrs']],
            'ror': [ror[x] for x in c['ror_perimeter'] + c['ror_other_active']],
        }
        per_record = {src: [record_values(src, x) for x in recs] for src, recs in records.items()}
        fields = {}
        for field in record_values('ror', {'acronyms': [], 'label_fr': None, 'label': None, 'label_en': None,
                                           'codes': [], 'types': [], 'website': None, 'city': None,
                                           'country': None, 'gps': None, 'start_year': None}):
            values = {src: [v for rv in per_record[src] for v in rv[field] if v not in (None, '')]
                      for src in SOURCES_ORDER}
            values = {src: list(dict.fromkeys(v)) for src, v in values.items()}
            entry = {src: v for src, v in values.items() if v}
            if field not in FIELDS_WITHOUT_STATUS:
                entry['status'] = field_status(values, field)
            fields[field] = entry

        # établissements : tutelles RNSR, tutelles CNRS (principales / secondaires), parents ROR
        participants = {tutelle_key_of_rnsr(t['id'], paysage): t['label'] for u in records['rnsr']
                        for t in u['participants']}
        etablissements = []
        for row in etabs_by_cluster.get(c['cluster'], []):
            key = row['tutelle']
            etablissements.append({
                'key': key,
                'label': row['tutelle_label'],
                'ror': [key[4:]] if key.startswith('ror:') else (paysage.get(key) or {}).get('rors', []),
                'seen_by': row['seen_by'],
                'rnsr': 'tutelle' if row['in_rnsr'] else ('participant' if key in participants else None),
                'rnsr_referent': row['referent'],
                'cnrs': row['cnrs_view'] if row['cnrs_view'] in ('main', 'secondary') else None,
                'ror_depth': row['ror_depth'],
            })
        known = {e['key'] for e in etablissements}
        for key, label in participants.items():
            if key not in known:
                etablissements.append({'key': key, 'label': tutelle_label(key, sources) or label,
                                       'ror': [key[4:]] if key.startswith('ror:') else (paysage.get(key) or {}).get('rors', []),
                                       'seen_by': 'rnsr_participant', 'rnsr': 'participant', 'rnsr_referent': None,
                                       'cnrs': None, 'ror_depth': None})

        ror_ext = defaultdict(list)
        for r in records['ror']:
            for t, v in r['external_ids'].items():
                ror_ext[t] += v
        identifiers = {
            'rnsr': c['rnsr_active'], 'rnsr_fermes': c['rnsr_old'], 'cnrs': c['cnrs'],
            'ror': c['ror_perimeter'] + c['ror_other_active'], 'ror_inactifs': c['ror_inactive'],
            'paysage': c['paysage'],
            'idref': sorted({i for u in records['rnsr'] for i in u['idref'] if i}),
            'hal': sorted({i for u in records['rnsr'] for i in u['hal'] if i}),
            **{t: sorted(set(v)) for t, v in ror_ext.items() if t in ('wikidata', 'isni', 'grid')},
        }
        identifiers = {k: v for k, v in identifiers.items() if v}

        flags = []
        if c['category'].startswith('conflict'):
            flags.append(c['category'])
        # absence du RNSR ou de ROR ; celle du CNRS n'a de sens que s'il est tutelle (plus bas)
        for src in ('rnsr', 'ror'):
            if not records[src]:
                flags.append(f'absent_{src}')
        if c['category'] == 'rnsr_ror_out_of_perimeter':
            flags.append('ror_hors_perimetre')
        for field in ('libelle_long_fr', 'acronyme', 'code_unite', 'site_web', 'localite'):
            if fields[field].get('status') in ('different', 'partial'):
                flags.append(f'{field}_{fields[field]["status"]}')
        # un établissement que toutes les sources présentes ne donnent pas
        present = {src for src in SOURCES_ORDER if records[src]}
        if len(present) > 1 and any(set(e['seen_by'].split('+')) != present for e in etablissements
                                    if e['seen_by'] != 'rnsr_participant'):
            flags.append('etablissements_divergents')
        if not records['cnrs'] and any(e['key'] == cnrs_key and e['seen_by'] != 'rnsr_participant'
                                       for e in etablissements):
            flags.append('tutelle_cnrs_sans_fiche_cnrs')
        referents = sorted({t['label'] or t['id'] for u in records['rnsr'] for t in u['tutelles'] if t['referent'] is True})

        entries.append({
            'structure_key': c['key'],
            'category': c['category'],
            'sources': [src for src in SOURCES_ORDER if records[src]],
            'in_rnsr': bool(records['rnsr']), 'in_cnrs': bool(records['cnrs']), 'in_ror': bool(records['ror']),
            'identifiants': identifiers,
            'champs': fields,
            'etablissements': etablissements,
            'etablissements_keys': sorted({e['key'] for e in etablissements}),
            'referent_rnsr': referents,
            'instituts_cnrs': {'principal': sorted({i for cu in records['cnrs'] for i in cu['institutes_main']}),
                               'secondaires': sorted({i for cu in records['cnrs'] for i in cu['institutes_secondary']})},
            'responsables': {'rnsr': [l for u in records['rnsr'] for l in u['leaders']],
                             'cnrs': [l for cu in records['cnrs'] for l in cu['leaders']]},
            'liens': [f"{e['a']}~{e['b']}[{e['source']}{' ' + e['detail'] if e['detail'] else ''}]" for e in c['edges']],
            'a_curer': flags,
        })
    return entries


def structure_csv_row(e: dict) -> dict:
    row = {k: e[k] for k in ('structure_key', 'category', 'sources', 'in_rnsr', 'in_cnrs', 'in_ror')}
    for k in ('rnsr', 'rnsr_fermes', 'cnrs', 'ror', 'ror_inactifs', 'paysage', 'idref', 'hal', 'wikidata', 'isni', 'grid'):
        row[f'id_{k}'] = e['identifiants'].get(k)
    for field, values in e['champs'].items():
        for src in SOURCES_ORDER:
            row[f'{field}_{src}'] = values.get(src)
        if 'status' in values:
            row[f'{field}_status'] = values['status']
    row['etablissements'] = [f"{x['label'] or x['key']} [{x['seen_by']}]" for x in e['etablissements']]
    row['etablissements_keys'] = e['etablissements_keys']
    row['referent_rnsr'] = e['referent_rnsr']
    row['institut_cnrs'] = e['instituts_cnrs']['principal']
    row['responsables_rnsr'] = [f"{l['name']} ({l['role']})" for l in e['responsables']['rnsr']]
    row['responsables_cnrs'] = [f"{l['name']} ({l['role']})" for l in e['responsables']['cnrs']]
    row['a_curer'] = e['a_curer']
    return row


def write_structures_files(entries: list, output_dir: str) -> None:
    """structures.jsonl (imbriqué), structures.csv (à plat), et un fichier par établissement."""
    write_jsonl(entries, os.path.join(output_dir, 'structures.jsonl'))
    rows = [structure_csv_row(e) for e in entries]
    # colonnes fixes et dans le même ordre pour toutes les lignes
    columns = []
    for row in rows:
        for k in row:
            if k not in columns:
                columns.append(k)
    rows = [{k: row.get(k) for k in columns} for row in rows]
    write_csv(rows, os.path.join(output_dir, 'structures.csv'))
    etab_dir = os.path.join(output_dir, 'structures_par_etablissement')
    os.makedirs(etab_dir, exist_ok=True)
    for f in os.listdir(etab_dir):
        if f.endswith('.csv'):
            os.remove(os.path.join(etab_dir, f))
    by_etab, labels = defaultdict(list), {}
    for e, row in zip(entries, rows):
        for x in e['etablissements']:
            by_etab[x['key']].append(row)
            labels[x['key']] = x['label']
    for key, etab_rows in by_etab.items():
        prefix = f"cnrs_label_{slugify(labels[key])}" if key.startswith('cnrs_label:') \
            else f"{key.replace(':', '_')}_{slugify(labels[key])}"
        write_csv(etab_rows, os.path.join(etab_dir, f'{prefix}.csv'))



def cluster_row(i: int, c: dict, sources: dict, perimeter: dict) -> dict:
    rnsr, ror = sources['rnsr'], sources['ror']
    all_rors = c['ror_perimeter'] + c['ror_other_active'] + c['ror_inactive']
    cnrs = [sources['cnrs'][x] for x in c['cnrs']]
    return {
        'cluster': i,
        'structure_key': c['key'],
        'category': c['category'],
        'sources_present': c['sources_present'],
        'rnsr_active': c['rnsr_active'],
        'rnsr_active_labels': labels_of(c['rnsr_active'], rnsr),
        'rnsr_level': [rnsr[x]['level'] for x in c['rnsr_active']],
        'rnsr_referent_labels': ['+'.join(referent_labels(rnsr[x])) for x in c['rnsr_active']],
        'rnsr_old': c['rnsr_old'],
        'rnsr_old_labels': labels_of(c['rnsr_old'], rnsr),
        'ror': all_rors,
        'ror_labels': labels_of(all_rors, ror),
        'ror_status': [(ror.get(x) or {}).get('status') for x in all_rors],
        'ror_types': ['+'.join((ror.get(x) or {}).get('types', [])) for x in all_rors],
        'ror_depth': [(perimeter.get(x) or {}).get('depth') for x in all_rors],
        'ror_parents_labels': ['+'.join(str(l) for l in labels_of((ror.get(x) or {}).get('parents', []), ror))
                               for x in all_rors],
        'ror_codes': [' '.join(f'{f} {n}' for f, n in sorted((ror.get(x) or {}).get('codes', []))) for x in all_rors],
        'cnrs': c['cnrs'],
        'cnrs_labels': [cu['label'] for cu in cnrs],
        'cnrs_acronyms': [cu['acronym'] for cu in cnrs],
        'cnrs_institutes': ['+'.join(cu['institutes_main']) for cu in cnrs],
        'cnrs_rnsr_declared': [cu['rnsr'] for cu in cnrs],
        'cnrs_tutelles_main': ['+'.join(t['label'] for t in cu['tutelles'] if t['type'] == 'main') for cu in cnrs],
        'link_sources': c['link_sources'],
        'edges': [f"{e['a']}~{e['b']}[{e['source']}{' ' + e['detail'] if e['detail'] else ''}]" for e in c['edges']],
    }


def run_diagnostic(output_dir: str = DEFAULT_OUTPUT_DIR, rnest_dir: Optional[str] = None) -> dict:
    """Diagnostic complet ; avec rnest_dir, écrit aussi la proposition d'initialisation RNeST (rnest_init)."""
    os.makedirs(output_dir, exist_ok=True)
    sources = load_sources()
    rnsr, ror, cnrs = sources['rnsr'], sources['ror'], sources['cnrs']
    cnrs_mapping = resolve_cnrs_tutelles(cnrs, rnsr, sources['paysage'])
    write_csv(cnrs_mapping, os.path.join(output_dir, 'cnrs_tutelles_mapping.csv'))

    esr = build_esr_etabs(rnsr, sources['paysage'])
    perimeter = build_ror_perimeter(ror, esr)
    logger.debug(f'{len(esr)} ROR établissements ESR, {len(perimeter)} ROR dans le périmètre')

    edges, rejected = build_links(sources, esr, perimeter)
    clusters = build_clusters(edges, sources, perimeter)

    # sorties par cluster
    rows_by_category = defaultdict(list)
    for i, c in enumerate(clusters):
        c['cluster'] = i
        rows_by_category[c['category']].append(cluster_row(i, c, sources, perimeter))
    write_jsonl(clusters, os.path.join(output_dir, 'clusters.jsonl'))
    for category, rows in rows_by_category.items():
        write_csv(rows, os.path.join(output_dir, f'{category}.csv'))

    # comparaison champ par champ des paires RNSR actif <-> ROR du périmètre (1-1)
    pair_rows, field_stats = [], defaultdict(Counter)
    for c in clusters:
        if c['category'] != 'rnsr_ror':
            continue
        unit, r = rnsr[c['rnsr_active'][0]], ror[c['ror_perimeter'][0]]
        comparison = compare_pair(unit, r, sources)
        details = comparison.pop('_details')
        for field, value in comparison.items():
            if field != 'label_score':
                field_stats[field][value] += 1
        pair_rows.append({'cluster': c['cluster'], 'rnsr': unit['id'], 'ror': r['id'],
                          'rnsr_level': unit['level'], 'link_sources': c['link_sources'],
                          **comparison, **details})
    write_csv(pair_rows, os.path.join(output_dir, 'pairs_rnsr_ror.csv'))

    # comparaison champ par champ RNSR actif <-> fiche CNRS (1-1, avec ou sans ROR)
    cnrs_pair_rows, cnrs_field_stats = [], defaultdict(Counter)
    for c in clusters:
        if len(c['rnsr_active']) != 1 or len(c['cnrs']) != 1:
            continue
        unit, cu = rnsr[c['rnsr_active'][0]], cnrs[c['cnrs'][0]]
        comparison = compare_rnsr_cnrs(unit, cu, sources)
        details = comparison.pop('_details')
        for field, value in comparison.items():
            if field != 'label_score':
                cnrs_field_stats[field][value] += 1
        cnrs_pair_rows.append({'cluster': c['cluster'], 'category': c['category'], 'rnsr': unit['id'],
                               'cnrs': cu['id'], 'rnsr_level': unit['level'], 'link_sources': c['link_sources'],
                               **comparison, **details})
    write_csv(cnrs_pair_rows, os.path.join(output_dir, 'pairs_rnsr_cnrs.csv'))
    write_csv(rejected, os.path.join(output_dir, 'rejected_links.csv'))
    contradictions = idref_paysage_contradictions(edges, sources)
    write_csv(contradictions, os.path.join(output_dir, 'idref_paysage_contradictions.csv'))

    # vue par tutelle et tutelle référente
    paysage_ror_index = build_paysage_ror_index(sources['paysage'])
    tutelle_rows, tutelle_summary = build_tutelle_view(clusters, sources, esr, paysage_ror_index)
    write_tutelle_files(tutelle_rows, tutelle_summary, output_dir)
    structures = build_structures_view(clusters, sources, tutelle_rows)
    write_structures_files(structures, output_dir)
    anomalies = referent_anomalies(rnsr)
    write_csv(anomalies, os.path.join(output_dir, 'referent_anomalies.csv'))

    # unités RNSR actives sous tutelle CNRS sans fiche dans l'annuaire
    cnrs_key = next((r['key'] for r in cnrs_mapping if r['cnrs_label'] == 'CNRS'), None)
    rnsr_cnrs_missing = []
    for c in clusters:
        if c['cnrs']:
            continue
        for x in c['rnsr_active']:
            unit = rnsr[x]
            if cnrs_key and any(tutelle_key_of_rnsr(t['id'], sources['paysage']) == cnrs_key for t in unit['tutelles']):
                rnsr_cnrs_missing.append({'cluster': c['cluster'], 'category': c['category'], 'rnsr': x,
                                          'label': unit['label'], 'acronym': unit['acronym'], 'level': unit['level'],
                                          'codes': ' '.join(f'{f} {n}' for f, n in sorted(unit['codes'])),
                                          'city': unit['city']})
    write_csv(rnsr_cnrs_missing, os.path.join(output_dir, 'rnsr_cnrs_tutelle_without_cnrs_unit.csv'))

    # synthèse
    rnsr_active = [u for u in rnsr.values() if u['active']]
    level_by_category = defaultdict(Counter)
    cnrs_type_by_category = defaultdict(Counter)
    for c in clusters:
        for x in c['rnsr_active']:
            level_by_category[c['category']][rnsr[x]['level']] += 1
        for x in c['cnrs']:
            cnrs_type_by_category[c['category']][cnrs[x]['code_type']] += 1
    summary = {
        'date': TODAY,
        'idref_source': 'sparql' if sources['idref'] is not None else 'rnsr_dump',
        'sources': {
            'rnsr_total': len(rnsr),
            'rnsr_active': len(rnsr_active),
            'rnsr_active_by_level': dict(Counter(u['level'] for u in rnsr_active)),
            'ror_total': len(ror),
            'ror_fr_active': sum(1 for r in ror.values() if r['active'] and r['country'] == 'FR'),
            'paysage_total': len(sources['paysage']),
            'cnrs_total': len(cnrs),
            'cnrs_by_code_type': dict(Counter(cu['code_type'] for cu in cnrs.values())),
            'cnrs_with_rnsr_declared': sum(1 for cu in cnrs.values() if cu['rnsr']),
            'cnrs_rnsr_declared_active': sum(1 for cu in cnrs.values() if (rnsr.get(cu['rnsr']) or {}).get('active')),
            'cnrs_rnsr_declared_inactive': sum(1 for cu in cnrs.values()
                                               if cu['rnsr'] in rnsr and not rnsr[cu['rnsr']]['active']),
            'cnrs_rnsr_declared_unknown': sum(1 for cu in cnrs.values() if cu['rnsr'] and cu['rnsr'] not in rnsr),
        },
        'cnrs_tutelles_mapping': {
            'labels': len(cnrs_mapping),
            'labels_by_method': dict(Counter(str(r['method']) for r in cnrs_mapping)),
            'tutelle_mentions_mapped': sum(r['nb_cnrs_units'] for r in cnrs_mapping if r['key']),
            'tutelle_mentions_total': sum(r['nb_cnrs_units'] for r in cnrs_mapping),
            'cnrs_key': cnrs_key,
        },
        'perimeter': {
            'esr_etabs': len(esr),
            'esr_etabs_by_origin': dict(Counter('+'.join(sorted(e['origins'])) for e in esr.values())),
            'ror_perimeter': len(perimeter),
            'ror_perimeter_by_depth': dict(Counter(p['depth'] for p in perimeter.values())),
            'ror_perimeter_by_type': dict(Counter('+'.join(sorted(ror[x]['types'])) for x in perimeter)),
            'ror_perimeter_with_code': sum(1 for x in perimeter if ror[x]['codes']),
        },
        'links': {
            'edges_by_source': dict(Counter(e['source'] for e in edges)),
            'rejected_by_reason': dict(Counter(f"{e['source']}:{e['reason']}" for e in rejected)),
            'idref_paysage_contradictions': len(contradictions),
        },
        'clusters': {
            'total': len(clusters),
            'by_category': dict(Counter(c['category'] for c in clusters)),
            'by_sources_present': dict(Counter(c['sources_present'] for c in clusters)),
            'cnrs_code_type_by_category': {k: dict(v) for k, v in cnrs_type_by_category.items()},
            'rnsr_level_by_category': {k: dict(v) for k, v in level_by_category.items()},
            'rnsr_ror_by_link_sources': dict(Counter('+'.join(c['link_sources'])
                                                     for c in clusters if c['category'] == 'rnsr_ror')),
        },
        'fields_rnsr_ror': {k: dict(v) for k, v in field_stats.items()},
        'fields_rnsr_cnrs': {k: dict(v) for k, v in cnrs_field_stats.items()},
        'tutelles': {
            'nb_tutelles': len(tutelle_summary),
            'rows_by_agreement': dict(Counter(r['agreement'] for r in tutelle_rows)),
            'rows_by_seen_by': dict(Counter(r['seen_by'] for r in tutelle_rows)),
            'rows_by_cnrs_view': dict(Counter(r['cnrs_view'] for r in tutelle_rows)),
            'rnsr_cnrs_tutelle_without_cnrs_unit': len(rnsr_cnrs_missing),
        },
        'structures': {
            'total': len(structures),
            'by_sources': dict(Counter('+'.join(e['sources']) for e in structures)),
            'a_curer': dict(Counter(f for e in structures for f in e['a_curer'])),
            'status_by_field': {f: dict(Counter(e['champs'][f].get('status') for e in structures))
                                for f in FIELD_NORMALIZERS if f not in FIELDS_WITHOUT_STATUS},
        },
        'referent': {
            'rnsr_active_with_field': sum(1 for u in rnsr_active if u['referent_known']),
            'rnsr_active_with_one_referent': sum(1 for u in rnsr_active if len(u['referents']) == 1),
            'anomalies': len(anomalies),
        },
    }
    if rnest_dir:
        from project.server.main.rnest_init import build_rnest_init
        summary['rnest'] = build_rnest_init(clusters, sources, esr, paysage_ror_index, rnest_dir)
    with open(os.path.join(output_dir, 'summary.json'), 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    logger.info(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Diagnostic RNSR / ROR / annuaire CNRS / paysage')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    run_diagnostic(args.output_dir)
