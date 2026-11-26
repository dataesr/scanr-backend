"""
Données d'initialisation de RNeST à partir du RNSR, de l'annuaire CNRS et de ROR.

Il ne s'agit pas d'un rapprochement avec une base RNeST existante, mais d'un jeu de
données de départ, aux noms de champs du modèle RNeST (structure_libelle, localisation,
rattachement, referent, responsabilite).

Aucune valeur n'est choisie : pour chaque champ, toutes les valeurs distinctes données
par les sources, chacune avec la liste des sources qui la donnent :
    "libelle_long_fr": [{"valeur": "...", "sources": ["cnrs", "ror"]},
                        {"valeur": "...", "sources": ["rnsr"]}]
Les valeurs sont comparées telles quelles (espaces de bord retirés), sans normalisation,
sauf le pays (ramené au code ISO 3166 alpha-2) et le code d'unité (« UMR 6164 »).
Les valeurs sont triées par nombre de sources, puis dans l'ordre rnsr, cnrs, ror.

Les liens et les clusters viennent de diagnostic_rnsr :
- un cluster conflit_multi_rnsr est éclaté en une structure par RNSR actif ; les fiches
  ROR / CNRS revendiquées par plusieurs RNSR n'y sont gardées que comme candidates ;
- comme la reprise RNeST du RNSR, les équipes internes ne sont pas reprises
  (EXCLUDED_RNSR_LEVELS), et sont listées dans structures_exclues.jsonl.gz.

Sorties (RNEST_OUTPUT_DIR, puis production/rnest/ sur S3 avec --upload) :
- structures.jsonl.gz      une structure par ligne
- etablissements.jsonl.gz  les établissements cités par les rattachements
- structures_exclues.jsonl.gz
- rapport.json

Usage :
    python -m project.server.main.rnest_init [--output-dir DIR] [--upload]
"""
import argparse
import gzip
import json
import os
from collections import Counter, defaultdict
from typing import Optional

from project.server.main.logger import get_logger
from project.server.main import diagnostic_rnsr as diag

logger = get_logger(__name__)

RNEST_OUTPUT_DIR = '/upw_data/scanr/orga_ref/rnest'
S3_CONTAINER = 'scanr-data'
S3_PREFIX = 'production/rnest'

# Niveaux RNSR non repris (comme EI dans la reprise RNeST : apps/api/database/rnsr/import.ts)
EXCLUDED_RNSR_LEVELS = {'Equipe interne'}

# Champs de la vue consolidée (diagnostic_rnsr.record_values) -> nom du champ en sortie
FIELDS = {
    'acronyme': 'acronyme',
    'libelle_long_fr': 'libelle_long_fr',
    'libelle_en': 'libelle_long_en',
    'code_unite': 'code_unite',
    'type': 'type',
    'site_web': 'site_web',
    'adresse': 'adresse',
    'code_postal': 'code_postal',
    'localite': 'localite',
    'pays': 'pays',
    'gps': 'gps',
    'annee_debut': 'annee_debut',
}


# ---------------------------------------------------------------------------
# Valeurs et sources
# ---------------------------------------------------------------------------

def to_iso2(country) -> Optional[str]:
    """'FR', 'FRA', 'France' -> 'FR' ; codes ISO2 / ISO3 / noms anglais via pycountry."""
    if not isinstance(country, str) or not country.strip():
        return None
    c = country.strip()
    if diag.normalize_text(c) in ('france', 'fr', 'fra'):
        return 'FR'
    try:
        import pycountry
        if len(c) == 2:
            found = pycountry.countries.get(alpha_2=c.upper())
        elif len(c) == 3:
            found = pycountry.countries.get(alpha_3=c.upper())
        else:
            found = pycountry.countries.lookup(c)
        return found.alpha_2 if found else None
    except Exception:
        return None


def values_with_sources(per_source: dict) -> list:
    """
    {source: [valeurs]} -> [{'valeur': v, 'sources': [...]}], une entrée par valeur distincte
    (comparaison exacte), triée par nombre de sources puis dans l'ordre rnsr, cnrs, ror.
    """
    out = {}
    for src in diag.SOURCES_ORDER:
        for v in per_source.get(src) or []:
            if isinstance(v, str):
                v = v.strip()
            if v in (None, '', [], {}):
                continue
            key = json.dumps(v, sort_keys=True, ensure_ascii=False)
            entry = out.setdefault(key, {'valeur': v, 'sources': []})
            if src not in entry['sources']:
                entry['sources'].append(src)
    return sorted(out.values(), key=lambda e: -len(e['sources']))


# ---------------------------------------------------------------------------
# Éclatement des conflits
# ---------------------------------------------------------------------------

def split_conflicts(clusters: list, sources: dict) -> list:
    """
    Un cluster à plusieurs RNSR actifs devient une structure par RNSR. Une fiche ROR ou CNRS
    y est rattachée si elle est liée à ce seul RNSR (directement ou via une fiche paysage) ;
    sinon elle n'est que candidate.
    """
    out = []
    for c in clusters:
        if c['category'] != 'conflict_multi_rnsr':
            out.append(dict(c, candidates={'ror': [], 'cnrs': []}))
            continue
        neighbors = defaultdict(set)
        for e in c['edges']:
            neighbors[e['a']].add(e['b'])
            neighbors[e['b']].add(e['a'])
        reached = {}
        for rnsr_id in c['rnsr_active']:
            node = f'rnsr:{rnsr_id}'
            direct = set(neighbors[node])
            via_paysage = {n2 for n in direct if n.startswith('paysage:') for n2 in neighbors[n]}
            reached[rnsr_id] = {n for n in direct | via_paysage if n.startswith(('ror:', 'cnrs:'))}
        claims = Counter(n for nodes in reached.values() for n in nodes)
        for rnsr_id in c['rnsr_active']:
            own = {n for n in reached[rnsr_id] if claims[n] == 1}
            shared = reached[rnsr_id] - own
            rors = {n[4:] for n in own if n.startswith('ror:')}
            cnrs = [n[5:] for n in sorted(own) if n.startswith('cnrs:')]
            sub = dict(c)
            sub.update({
                'key': f'rnsr:{rnsr_id}',
                'rnsr_active': [rnsr_id],
                'rnsr_old': [],
                'ror_perimeter': [x for x in c['ror_perimeter'] if x in rors],
                'ror_other_active': [x for x in c['ror_other_active'] if x in rors],
                'ror_inactive': [],
                'cnrs': cnrs,
                'edges': [e for e in c['edges']
                          if all(n in own or n == f'rnsr:{rnsr_id}' or n.startswith('paysage:')
                                 for n in (e['a'], e['b']))],
                'candidates': {'ror': sorted(n[4:] for n in shared if n.startswith('ror:')),
                               'cnrs': sorted(n[5:] for n in shared if n.startswith('cnrs:'))},
            })
            out.append(sub)
    for i, c in enumerate(out):
        c['cluster'] = i
    return out


# ---------------------------------------------------------------------------
# Construction des structures et établissements
# ---------------------------------------------------------------------------

def split_cnrs_name(name: str) -> tuple:
    """'Marc DHENAIN' -> ('Marc', 'DHENAIN') : le nom est en capitales dans l'annuaire."""
    tokens = (name or '').split()
    upper = [t for t in tokens if t.isupper() and len(t) > 1]
    if not upper:
        return None, name
    prenom = ' '.join(t for t in tokens if t not in upper) or None
    return prenom, ' '.join(upper)


def person_key(name: str) -> str:
    """Clé de rapprochement d'une personne entre sources : mots du nom, triés."""
    return ' '.join(sorted(diag.normalize_text(name).split()))


def responsables_of(records: dict) -> list:
    """Responsables courants (RNSR : sans date de fin ; annuaire CNRS), regroupés par personne."""
    people = {}
    for u in records['rnsr']:
        for l in u['leaders']:
            name = l.get('name') or ''
            p = people.setdefault(person_key(name), {'personne': name, 'sources': [], 'par_source': defaultdict(list)})
            p['par_source']['rnsr'].append({'nom': l.get('nom'), 'prenom': l.get('prenom'), 'role': l.get('role'),
                                            'idref': l.get('idref'), 'debut': l.get('start') or None})
    for cu in records['cnrs']:
        for l in cu['leaders']:
            name = l.get('name') or ''
            prenom, nom = split_cnrs_name(name)
            p = people.setdefault(person_key(name), {'personne': name, 'sources': [], 'par_source': defaultdict(list)})
            p['par_source']['cnrs'].append({'nom': nom, 'prenom': prenom, 'role': l.get('role')})
    out = []
    for p in people.values():
        p['sources'] = [src for src in diag.SOURCES_ORDER if p['par_source'].get(src)]
        p['par_source'] = dict(p['par_source'])
        out.append(p)
    return sorted(out, key=lambda p: -len(p['sources']))


def to_rnest_structure(entry: dict, c: dict, sources: dict, cnrs_key: Optional[str]) -> dict:
    rnsr, ror, cnrs, paysage = sources['rnsr'], sources['ror'], sources['cnrs'], sources['paysage']
    records = {
        'rnsr': [rnsr[x] for x in c['rnsr_active']],
        'cnrs': [cnrs[x] for x in c['cnrs']],
        'ror': [ror[x] for x in c['ror_perimeter'] + c['ror_other_active']],
    }
    champs = {}
    for field, name in FIELDS.items():
        per_source = {src: v for src, v in entry['champs'].get(field, {}).items() if src in diag.SOURCES_ORDER}
        if field == 'pays':
            per_source = {src: [to_iso2(x) or x for x in v] for src, v in per_source.items()}
        if field == 'gps':
            per_source = {src: [dict(zip(('latitude', 'longitude'), map(float, x.split(',')))) for x in v]
                          for src, v in per_source.items()}
        champs[name] = values_with_sources(per_source)
    champs['idref'] = values_with_sources({'rnsr': [i for u in records['rnsr'] for i in u['idref']]})
    champs['hal'] = values_with_sources({'rnsr': [i for u in records['rnsr'] for i in u['hal']]})
    for t in ('isni', 'wikidata'):
        champs[t] = values_with_sources({'ror': [i for r in records['ror'] for i in r['external_ids'].get(t, [])]})
    champs['institut_cnrs_principal'] = values_with_sources({'cnrs': [i for cu in records['cnrs'] for i in cu['institutes_main']]})
    champs['institut_cnrs_secondaire'] = values_with_sources({'cnrs': [i for cu in records['cnrs'] for i in cu['institutes_secondary']]})
    champs['referent'] = values_with_sources({'rnsr': [diag.tutelle_key_of_rnsr(t, paysage)
                                                       for u in records['rnsr'] for t in u['referents']]})

    # rattachements : tous les établissements, avec le rôle que leur donne chaque source
    rattachements = []
    for e in entry['etablissements']:
        par_source = {}
        if e['rnsr']:
            par_source['rnsr'] = {'relation': e['rnsr'], 'referent': e['rnsr_referent']}
        if e['cnrs']:
            par_source['cnrs'] = {'tutelle': 'principale' if e['cnrs'] == 'main' else 'secondaire'}
            if e['key'] == cnrs_key:
                par_source['cnrs']['code_unite'] = c['cnrs']
        if e['ror_depth']:
            par_source['ror'] = {'profondeur': e['ror_depth']}
        rattachements.append({
            'etablissement': e['key'],
            'etablissement_libelle': e['label'],
            'sources': [src for src in diag.SOURCES_ORDER if src in par_source],
            'par_source': par_source,
        })
    rattachements.sort(key=lambda r: -len(r['sources']))

    flags = list(entry['a_curer'])
    if len(c['ror_perimeter'] + c['ror_other_active']) > 1:
        flags.append('plusieurs_ror')
    if c['candidates']['ror'] or c['candidates']['cnrs']:
        flags.append('candidats_partages')
    if c['rnsr_old'] and not c['rnsr_active']:
        flags.append('rnsr_ferme')
    if any(r['sources'] == ['ror'] for r in rattachements):
        flags.append('rattachement_ror_seul')
    if any(len(v) > 1 for k, v in champs.items() if k in ('libelle_long_fr', 'acronyme', 'code_unite')):
        flags.append('valeurs_multiples')

    return {
        'cle': entry['structure_key'],
        'sources': entry['sources'],
        'categorie_diagnostic': entry['category'],
        'identifiants': {
            'rnsr': c['rnsr_active'],
            'cnrs': c['cnrs'],
            'ror': c['ror_perimeter'] + c['ror_other_active'],
            'rnsr_fermes': c['rnsr_old'],
            'ror_inactifs': c['ror_inactive'],
            'ror_candidats': c['candidates']['ror'],
            'cnrs_candidats': c['candidates']['cnrs'],
            'paysage': c['paysage'],
        },
        'champs': champs,
        'rattachements': rattachements,
        'responsables': responsables_of(records),
        'liens': entry['liens'],
        'a_verifier': sorted(set(flags)),
    }


def to_rnest_etablissement(key: str, sources: dict, esr: dict, cnrs_labels: dict) -> dict:
    paysage, ror = sources['paysage'], sources['ror']
    if key.startswith('cnrs_label:'):
        return {'cle': key, 'paysage_id': None, 'ror_id': None, 'siren': None, 'uai': None,
                'libelle': key[len('cnrs_label:'):], 'acronyme': None, 'libelle_en': None, 'pays': None,
                'ville': None, 'esr': False, 'libelles_cnrs': [key[len('cnrs_label:'):]],
                'a_verifier': ['non_rapproche_paysage']}
    p = paysage.get(key)
    if p:
        rors = p['rors']
        r = ror.get(rors[0]) if len(rors) == 1 else None
        flags = [] if len(rors) <= 1 else ['plusieurs_ror']
        if not rors:
            flags.append('sans_ror')
        return {
            'cle': key, 'paysage_id': key, 'ror_id': rors[0] if len(rors) == 1 else None,
            'siren': p['siren'][0] if len(p['siren']) == 1 else None,
            'uai': p['uai'][0] if len(p['uai']) == 1 else None,
            'libelle': p['label'], 'acronyme': p['acronym'], 'libelle_en': p['label_en'],
            'pays': to_iso2(p['iso3']) or (r or {}).get('country'), 'ville': p['city'],
            'esr': any(x in esr for x in rors),
            'autres_identifiants': {'ror': rors if len(rors) > 1 else [], 'siren': p['siren'] if len(p['siren']) > 1 else [],
                                    'uai': p['uai'] if len(p['uai']) > 1 else [], 'wikidata': p['wikidata']},
            'libelles_cnrs': sorted(cnrs_labels.get(key, [])),
            'a_verifier': flags,
        }
    ror_id = key[4:] if key.startswith('ror:') else diag.normalize_ror(key)
    r = ror.get(ror_id) or {}
    return {'cle': key, 'paysage_id': None, 'ror_id': ror_id, 'siren': None, 'uai': None,
            'libelle': r.get('label'), 'acronyme': (r.get('acronyms') or [None])[0], 'libelle_en': r.get('label_en'),
            'pays': r.get('country'), 'ville': r.get('city'), 'esr': ror_id in esr,
            'libelles_cnrs': sorted(cnrs_labels.get(key, [])), 'a_verifier': ['sans_paysage']}


def write_gz_jsonl(data: list, path: str) -> None:
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        for elt in data:
            f.write(json.dumps(elt, ensure_ascii=False, default=list) + '\n')


def build_rnest_init(clusters: list, sources: dict, esr: dict, paysage_ror_index: dict,
                     output_dir: str = RNEST_OUTPUT_DIR) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    rnsr = sources['rnsr']
    split = split_conflicts(clusters, sources)
    tutelle_rows, _ = diag.build_tutelle_view(split, sources, esr, paysage_ror_index)
    entries = diag.build_structures_view(split, sources, tutelle_rows)
    cnrs_key = next((t['id'] for cu in sources['cnrs'].values() for t in cu['tutelles']
                     if t['label'] == 'CNRS' and t['id']), None)

    structures, excluded = [], []
    for c, entry in zip(split, entries):
        levels = {rnsr[x]['level'] for x in c['rnsr_active']}
        if levels and levels <= EXCLUDED_RNSR_LEVELS:
            excluded.append({'cle': entry['structure_key'], 'motif': 'niveau_rnsr_non_repris',
                             'niveau': sorted(levels), 'sources': entry['sources'],
                             'libelle': entry['champs']['libelle_long_fr'], 'identifiants': entry['identifiants']})
            continue
        structures.append(to_rnest_structure(entry, c, sources, cnrs_key))

    # un identifiant porté par plusieurs structures (RNeST : rnsr_id et ror_id sont UNIQUE)
    for kind in ('rnsr', 'ror', 'cnrs'):
        counts = Counter(x for st in structures for x in st['identifiants'][kind])
        for st in structures:
            if any(counts[x] > 1 for x in st['identifiants'][kind]):
                st['a_verifier'] = sorted(set(st['a_verifier']) | {f'{kind}_partage'})

    # établissements cités
    cnrs_labels = defaultdict(set)
    for cu in sources['cnrs'].values():
        for t in cu['tutelles']:
            if t['id']:
                cnrs_labels[t['id']].add(t['label'])
    etab_keys = sorted({r['etablissement'] for st in structures for r in st['rattachements']}
                       | {v['valeur'] for st in structures for v in st['champs']['referent']})
    etablissements = [to_rnest_etablissement(k, sources, esr, cnrs_labels) for k in etab_keys]
    nb_structures = Counter(r['etablissement'] for st in structures for r in st['rattachements'])
    for e in etablissements:
        e['nb_structures'] = nb_structures.get(e['cle'], 0)

    write_gz_jsonl(structures, os.path.join(output_dir, 'structures.jsonl.gz'))
    write_gz_jsonl(etablissements, os.path.join(output_dir, 'etablissements.jsonl.gz'))
    write_gz_jsonl(excluded, os.path.join(output_dir, 'structures_exclues.jsonl.gz'))

    champ_names = list(structures[0]['champs']) if structures else []
    rapport = {
        'date': diag.TODAY,
        'regles': {
            'comparaison': 'valeurs exactes ; pays en ISO alpha-2, code d\'unité normalisé',
            'niveaux_rnsr_non_repris': sorted(EXCLUDED_RNSR_LEVELS),
        },
        'structures': len(structures),
        'structures_par_sources': dict(Counter('+'.join(st['sources']) for st in structures)),
        'structures_exclues': dict(Counter(e['motif'] for e in excluded)),
        'a_verifier': dict(Counter(f for st in structures for f in st['a_verifier'])),
        'champs': {
            name: {
                'renseigne': sum(1 for st in structures if st['champs'][name]),
                'plusieurs_valeurs': sum(1 for st in structures if len(st['champs'][name]) > 1),
                'nb_valeurs': dict(sorted(Counter(len(st['champs'][name]) for st in structures).items())),
            } for name in champ_names
        },
        'responsables': {
            'structures_avec': sum(1 for st in structures if st['responsables']),
            'personnes_par_sources': dict(Counter('+'.join(p['sources']) for st in structures for p in st['responsables'])),
        },
        'rattachements_par_sources': dict(Counter('+'.join(r['sources']) for st in structures for r in st['rattachements'])),
        'etablissements': len(etablissements),
        'etablissements_a_verifier': dict(Counter(f for e in etablissements for f in e['a_verifier'])),
    }
    with open(os.path.join(output_dir, 'rapport.json'), 'w') as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)
    logger.info(f'rnest : {len(structures)} structures, {len(etablissements)} établissements, '
                f'{len(excluded)} exclues -> {output_dir}')
    return rapport


RNEST_FILES = ['structures.jsonl.gz', 'etablissements.jsonl.gz', 'structures_exclues.jsonl.gz', 'rapport.json']


def upload_rnest_init(output_dir: str = RNEST_OUTPUT_DIR, prefix: str = S3_PREFIX) -> None:
    """Envoi sur S3 dans production/rnest/ (upload_object publie en public-read)."""
    from project.server.main.s3 import upload_object
    for name in RNEST_FILES:
        path = os.path.join(output_dir, name)
        logger.debug(f'upload {path} -> {S3_CONTAINER}/{prefix}/{name}')
        upload_object(container=S3_CONTAINER, source=path, destination=f'{prefix}/{name}')


def run_rnest_init(output_dir: str = RNEST_OUTPUT_DIR, diagnostic_dir: Optional[str] = diag.DEFAULT_OUTPUT_DIR,
                   upload: bool = False) -> dict:
    """Calcule le diagnostic (écrit dans diagnostic_dir), puis les données d'initialisation RNeST."""
    rapport = diag.run_diagnostic(diagnostic_dir, rnest_dir=output_dir)['rnest']
    if upload:
        upload_rnest_init(output_dir)
    return rapport


CNRS_MIN_UNITS = 1000          # en dessous, le scraping est jugé incomplet et n'écrase pas le S3
CNRS_MAX_ERROR_RATE = 0.05


def check_cnrs_scrape(path: str) -> int:
    """Contrôle le fichier scrapé avant de remplacer la version du S3 (lue par le diagnostic)."""
    nb, nb_err = 0, 0
    with open(path) as f:
        for line in f:
            if line.strip():
                nb += 1
                nb_err += 'error' in json.loads(line)
    if nb < CNRS_MIN_UNITS or nb_err > CNRS_MAX_ERROR_RATE * nb:
        raise ValueError(f'scraping CNRS incomplet ({nb} fiches, {nb_err} en erreur) : S3 non mis à jour')
    logger.debug(f'scraping CNRS : {nb} fiches, {nb_err} en erreur')
    return nb


def run_rnest_job(args: Optional[dict] = None) -> dict:
    """
    Job complet (lancé depuis views, route /rnest_init) :
      1. optionnel (cnrs_scrape) : scraping de l'annuaire CNRS, contrôle, envoi sur le S3 ;
      2. diagnostic RNSR / ROR / CNRS (dumps relus sur le S3) ;
      3. données d'initialisation RNeST, envoyées dans production/rnest/ (upload, par défaut oui).
    args : cnrs_scrape (False), cnrs_delay (pause en secondes entre deux requêtes à l'annuaire CNRS, 3.0),
           output_dir, diagnostic_dir. Les fichiers RNeST sont toujours envoyés sur le S3.
    """
    from project.server.main import cnrs_annuaire
    args = args or {}
    if args.get('cnrs_scrape'):
        output = cnrs_annuaire.DEFAULT_OUTPUT
        cnrs_annuaire.scrape_cnrs_annuaire(output, delay=float(args.get('cnrs_delay', cnrs_annuaire.DEFAULT_DELAY)))
        check_cnrs_scrape(output)
        cnrs_annuaire.upload_cnrs_annuaire(output)
    return run_rnest_init(args.get('output_dir', RNEST_OUTPUT_DIR),
                          args.get('diagnostic_dir', diag.DEFAULT_OUTPUT_DIR),
                          upload=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Données d'initialisation RNeST (RNSR / CNRS / ROR)")
    parser.add_argument('--output-dir', default=RNEST_OUTPUT_DIR)
    parser.add_argument('--diagnostic-dir', default=diag.DEFAULT_OUTPUT_DIR)
    parser.add_argument('--upload', action='store_true', help='envoie les fichiers dans production/rnest/')
    parser.add_argument('--upload-only', action='store_true', help='envoie les fichiers déjà générés, sans recalcul')
    args = parser.parse_args()
    if args.upload_only:
        upload_rnest_init(args.output_dir)
    else:
        run_rnest_init(args.output_dir, args.diagnostic_dir, args.upload)
