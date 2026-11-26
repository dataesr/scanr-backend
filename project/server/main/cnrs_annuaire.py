"""
Récupération des unités de l'annuaire des laboratoires du CNRS (https://annuaire.cnrs.fr).

L'annuaire n'offre ni export ni API : on passe par le formulaire de recherche avancée,
puis on lit la fiche « Description » de chaque unité.

Attention : le robots.txt de annuaire.cnrs.fr interdit l'indexation de tout le site
(sauf l'accueil) et aucune licence de réutilisation n'est affichée. Usage exploratoire
uniquement, en attendant les données transmises par le CNRS : requêtes séquentielles,
avec une pause entre chaque, et pas de redistribution des données.

Fonctionnement du site :
- chaque réponse renvoie un en-tête X-TOKEN, à renvoyer dans le champ `xtoken` du POST suivant ;
- chaque page de formulaire porte un `uuid` à renvoyer avec la recherche ;
- une recherche renvoie au plus 2500 résultats (sans pagination côté serveur) : on
  énumère les codes d'unité par préfixe, et on découpe un préfixe qui atteint la limite ;
- les liens vers les fiches (uuid de ressource) sont liés à la session.

Les liens « Contact Lab » / « Press and Communication Contact » mènent à des formulaires
de contact : ils ne sont pas suivis.

Usage :
    python -m project.server.main.cnrs_annuaire --output /tmp/cnrs_annuaire.jsonl [--limit 10] [--delay 3] [--upload]
    python -m project.server.main.cnrs_annuaire --output /tmp/cnrs_annuaire.jsonl --upload-only
"""
import argparse
import gzip
import json
import shutil
import re
import string
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from project.server.main.logger import get_logger

logger = get_logger(__name__)

BASE_URL = 'https://annuaire.cnrs.fr'
SEARCH_FORM_URL = f'{BASE_URL}/NavigationServlet?pageName=recherche&type=UNITE'
SEARCH_URL = f'{BASE_URL}/NavigationServlet?pageName=resultat'
LANGUAGE_URL = f'{BASE_URL}/NavigationServlet?pageName=SwitchLanguage'
MAX_RESULTS = 2500
DEFAULT_DELAY = 3.0
DEFAULT_OUTPUT = '/upw_data/scanr/orga_ref/cnrs_annuaire.jsonl'
S3_CONTAINER = 'scanr-data'
S3_DESTINATION = 'production/cnrs_annuaire.jsonl.gz'
# premiers caractères des codes d'unité (UMR, UPR, UAR, IRL, FR, GDR, EMR...)
CODE_ALPHABET = string.ascii_uppercase + string.digits
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; dataesr-exploration)',
    'Origin': BASE_URL,
    'Referer': SEARCH_FORM_URL,
}


def clean(text) -> Optional[str]:
    if text is None:
        return None
    text = re.sub(r'\s+', ' ', text).strip()
    return text or None


class AnnuaireSession:
    """Session HTTP avec suivi du jeton X-TOKEN et pause entre requêtes."""

    def __init__(self, delay: float = DEFAULT_DELAY, language: str = 'fr'):
        self.delay = delay
        self.language = language
        self.token = ''
        self.last_request = 0.0
        self.session = None
        self.reset()

    def reset(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.token = ''
        self.get(SEARCH_FORM_URL)
        if self.language:
            # le site s'affiche par défaut en anglais
            self.post(LANGUAGE_URL, {'xtoken': self.token, 'languageToUse': self.language,
                                     'languageUrlRedirect': SEARCH_FORM_URL})

    def _wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request = time.time()

    def _update_token(self, response):
        token = response.headers.get('X-TOKEN')
        if token:
            self.token = token

    def get(self, url: str) -> requests.Response:
        self._wait()
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        self._update_token(r)
        return r

    def post(self, url: str, data) -> requests.Response:
        self._wait()
        r = self.session.post(url, data=data, timeout=120)
        r.raise_for_status()
        self._update_token(r)
        return r


# ---------------------------------------------------------------------------
# Recherche et liste de résultats
# ---------------------------------------------------------------------------

def search_by_code_prefix(s: AnnuaireSession, prefix: str) -> list:
    """Unités dont le code commence par `prefix` (au plus MAX_RESULTS)."""
    form = s.get(SEARCH_FORM_URL)
    uuid = re.search(r'id="uuid" value="([^"]*)"', form.text)
    data = [
        ('xtoken', s.token), ('formType', 'UNITE'), ('pageName', 'resultat'),
        ('uuid', uuid.group(1) if uuid else ''), ('name', ''),
        ('codeUniteStr', 'startsWith'), ('codeUniteStr', prefix),
        ('intitule', 'startsWith'), ('intitule', ''),
        ('nomDirecteur', 'startsWith'), ('nomDirecteur', ''),
        ('codeRnsrStr', 'startsWith'), ('codeRnsrStr', ''),
    ]
    r = s.post(SEARCH_URL, data)
    if 'pageName=retour&error' in r.url:
        raise RuntimeError(f'recherche refusée pour le préfixe {prefix}')
    return parse_result_list(r.text)


def parse_result_list(html: str) -> list:
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table', id='tablePagination')
    if table is None:
        return []
    units = []
    for tr in table.find_all('tr'):
        tds = tr.find_all('td')
        if not tds:
            continue
        first = tds[0]
        description = first.find('a', string=re.compile('Description'))
        if description is None:
            description = next((a for a in first.find_all('a') if a.find('img', src=re.compile('telecharger'))), None)
        header = clean(' '.join(first.find_all(string=True, recursive=False)))
        header = re.split(r'\s(?:Director|Directeur)', header)[0] if header else None
        code = header.split(' - ')[0].strip() if header else None
        institutes = [clean(d.get_text()) for d in first.find_all('div', class_='d-none')]
        website = None
        if len(tds) > 1:
            link = tds[1].find('a', target='_blank')
            website = link.get('href') if link else None
        units.append({
            'code': code,
            'list_header': header,
            'list_institutes': [i for i in institutes if i],
            'website': website,
            'description_url': description.get('href') if description else None,
        })
    return units


def iter_unit_batches(s: AnnuaireSession, prefixes=None):
    """
    Énumère les unités par préfixe de code, en découpant les préfixes saturés.
    Produit (préfixe, résultats) : les liens vers les fiches ne sont valables que
    jusqu'à la recherche suivante, il faut donc les lire avant de passer au préfixe suivant.
    """
    queue = list(prefixes or CODE_ALPHABET)
    while queue:
        prefix = queue.pop(0)
        try:
            results = search_by_code_prefix(s, prefix)
        except RuntimeError:
            # jeton perdu (par ex. après une recherche sans résultat) : nouvelle session
            logger.debug(f'préfixe {prefix!r} : recherche refusée, nouvelle session')
            s.reset()
            results = search_by_code_prefix(s, prefix)
        logger.debug(f'préfixe {prefix!r} : {len(results)} unités')
        if len(results) >= MAX_RESULTS:
            queue = [prefix + c for c in CODE_ALPHABET] + queue
            continue
        yield prefix, results


# ---------------------------------------------------------------------------
# Fiche d'une unité
# ---------------------------------------------------------------------------

def parse_nested_list(ul) -> dict:
    """<ul><li>Libellé : <ul><li>a</li><li>b</li></ul></li></ul> -> {'Libellé': ['a', 'b']}."""
    result = {}
    for li in ul.find_all('li', recursive=False):
        sub = li.find('ul')
        if sub is None:
            result.setdefault('_items', []).append(clean(li.get_text()))
            continue
        label = clean(''.join(t for t in li.find_all(string=True, recursive=False)))
        if not label:
            label_tag = li.find('label')
            label = clean(label_tag.get_text()) if label_tag else '_items'
        label = label.rstrip(' :')
        result[label] = [clean(x.get_text()) for x in sub.find_all('li') if clean(x.get_text())]
    return result


def parse_location(div) -> dict:
    location = {}
    for child in div.find_all('div', recursive=False):
        key = (child.get('class') or ['?'])[0]
        link = child.find('a')
        location[key] = link.get('href') if link and key == 'urlSiteWebStructure' else clean(child.get_text())
    return location


def parse_unit_page(html: str) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    fiche = soup.find('div', id='ficheStructure')
    if fiche is None:
        return {}
    unit = {}
    identity = fiche.find('div', class_='identiteStructure')
    if identity:
        unit['identity'] = clean(identity.get_text())
        m = re.match(r'^(\S+)\s*-\s*(.*?)(?:\s*\(([^()]*)\))?$', unit['identity'])
        if m:
            unit['code'], unit['label'], unit['acronym'] = m.group(1), clean(m.group(2)), clean(m.group(3))
    text = fiche.get_text(' ')
    m = re.search(r'RNSR\s*:\s*([0-9]{9}[A-Z])', text)
    unit['rnsr'] = m.group(1) if m else None
    m = re.search(r'(?:Last modification|Dernière modification)\s*:\s*([0-9/]+)', text)
    unit['last_modified'] = m.group(1) if m else None

    # responsables : liens de la cellule « Director / Directeur »
    leaders = []
    first_table = fiche.find('table')
    if first_table:
        for span in first_table.find_all('span'):
            span_text = clean(span.get_text()) or ''
            role = re.match(r'^([^:]+):', span_text)
            for a in span.find_all('a'):
                if 'ressource' in (a.get('href') or '') and 'Contact' not in a.get_text():
                    name = clean(a.get_text())
                    name = re.sub(r'^(Monsieur|Madame|Mister|Mrs|Mr|Ms)\s+', '', name or '')
                    leaders.append({'role': clean(role.group(1)) if role else None, 'name': name})
    unit['leaders'] = leaders

    # blocs (Organisation / Implantations / Tutelles / Partenariats...)
    sections = {}
    for card in fiche.find_all('div', class_='custom-card', recursive=False):
        header = card.find('div', class_='card-header')
        title = clean(header.get_text()) if header else '?'
        body = card.find('div', class_='card-body')
        if body is None:
            continue
        section = {}
        if body.find('div', class_='wrapImplantation'):
            for sub in body.find_all('div', class_='custom-card'):
                sub_header = clean(sub.find('div', class_='card-header').get_text())
                section[sub_header] = [parse_location(w) for w in sub.find_all('div', class_='wrapImplantation')]
            if not section:
                section['_items'] = [parse_location(w) for w in body.find_all('div', class_='wrapImplantation')]
        else:
            for block in body.find_all('div', recursive=False) or [body]:
                label = block.find('label', recursive=False)
                ul = block.find('ul', recursive=False)
                if ul is None:
                    continue
                parsed = parse_nested_list(ul)
                key = clean(label.get_text()) if label else '_items'
                section[key] = parsed
            raw = clean(body.get_text(' '))
            if raw:
                section['_text'] = raw
        if section:
            sections[title] = section
    unit['sections'] = sections
    unit.update(flatten_sections(sections))
    return unit


def _norm(text) -> str:
    return unidecode(text or '').lower()


def flatten_sections(sections: dict) -> dict:
    """Champs à plat à partir des blocs de la fiche (libellés FR ou EN)."""
    flat = {'institutes_main': [], 'institutes_secondary': [], 'tutelles_main': [], 'tutelles_secondary': [],
            'main_location': None, 'other_locations': [], 'other_sections': {}}

    def lists(section):
        # {'label': [..]} directement, ou imbriqué sous un libellé de bloc
        for key, value in section.items():
            if isinstance(value, dict):
                yield from value.items()
            elif isinstance(value, list) and key != '_text':
                yield key, value

    for title, section in sections.items():
        t = _norm(title)
        if 'implantation' in t or 'location' in t:
            for key, locations in section.items():
                locations = [format_location(x) for x in locations if isinstance(x, dict)]
                if 'principal' in _norm(key) or 'main' in _norm(key):
                    flat['main_location'] = locations[0] if locations else None
                    flat['other_locations'] += locations[1:]
                else:
                    flat['other_locations'] += locations
        elif 'organisation' in t or 'organization' in t:
            for key, values in lists(section):
                secondary = 'second' in _norm(key)
                flat['institutes_secondary' if secondary else 'institutes_main'] += values
        elif 'tutelle' in t or 'supervisory' in t:
            for key, values in lists(section):
                secondary = 'second' in _norm(key)
                flat['tutelles_secondary' if secondary else 'tutelles_main'] += values
        else:
            flat['other_sections'][title] = section.get('_text')
    return flat


def format_location(raw: dict) -> dict:
    commune = raw.get('communeStructure') or ''
    # code postal français (5 chiffres) ; sinon la ligne reste dans `city`, et `raw` garde l'original
    m = re.match(r'^(?:(\d{5})\s+)?(.*?)\s*(?:-\s*([^-]+))?$', commune)
    return {
        'name': raw.get('libelleStructure'),
        'complement': raw.get('complementAdresseStructure'),
        'street': raw.get('voieStructure'),
        'postcode': m.group(1) if m else None,
        'city': clean(re.sub(r'\bCEDEX\b.*$', '', m.group(2))) if m else None,
        'country': clean(m.group(3)) if m else None,
        'website': raw.get('urlSiteWebStructure'),
        'raw': raw,
    }


def fetch_unit(s: AnnuaireSession, u: dict) -> dict:
    record = dict(u)
    if not u.get('description_url'):
        return record
    try:
        page = parse_unit_page(s.get(u['description_url']).text)
        if not page:
            raise RuntimeError('fiche vide (lien expiré ?)')
        record.update(page)
    except Exception as e:
        logger.warning(f"{u.get('code')} : échec de lecture de la fiche ({e})")
        record['error'] = str(e)
    return record


def upload_cnrs_annuaire(output: str = DEFAULT_OUTPUT, destination: str = S3_DESTINATION) -> str:
    """
    Compresse le fichier et l'envoie sur le S3 (variables S3_* nécessaires).
    Attention : upload_object publie en public-read.
    """
    from project.server.main.s3 import upload_object
    gz_path = output + '.gz'
    with open(output, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return upload_object(container=S3_CONTAINER, source=gz_path, destination=destination)


def scrape_cnrs_annuaire(output: str = DEFAULT_OUTPUT, delay: float = DEFAULT_DELAY,
                         limit: Optional[int] = None, prefixes=None, upload: bool = False) -> int:
    s = AnnuaireSession(delay=delay)
    seen, nb = set(), 0
    with open(output, 'w') as f:
        for prefix, results in iter_unit_batches(s, prefixes):
            for u in results:
                if not u['code'] or u['code'] in seen:
                    continue
                if limit is not None and nb >= limit:
                    break
                seen.add(u['code'])
                f.write(json.dumps(fetch_unit(s, u), ensure_ascii=False) + '\n')
                f.flush()
                nb += 1
                if nb % 50 == 0:
                    logger.debug(f'{nb} fiches lues')
            if limit is not None and nb >= limit:
                break
    logger.debug(f'{nb} unités écrites dans {output}')
    if upload:
        upload_cnrs_annuaire(output)
    return nb


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Récupération de l'annuaire des laboratoires CNRS")
    parser.add_argument('--output', default=DEFAULT_OUTPUT)
    parser.add_argument('--delay', type=float, default=DEFAULT_DELAY, help='pause entre requêtes (s)')
    parser.add_argument('--limit', type=int, default=None, help='nombre max de fiches lues (test)')
    parser.add_argument('--prefixes', default=None, help='préfixes de code séparés par des virgules (test)')
    parser.add_argument('--upload', action='store_true', help=f'envoyer le résultat sur le S3 ({S3_DESTINATION})')
    parser.add_argument('--upload-only', action='store_true', help='envoyer un fichier déjà récupéré, sans scraper')
    args = parser.parse_args()
    if args.upload_only:
        upload_cnrs_annuaire(args.output)
    else:
        scrape_cnrs_annuaire(args.output, args.delay, args.limit,
                             args.prefixes.split(',') if args.prefixes else None, args.upload)
