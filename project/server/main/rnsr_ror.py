import json
from datetime import datetime
from collections import defaultdict, Counter
from typing import Optional

import pandas as pd

from project.server.main.s3 import upload_object
from project.server.main.utils import identifier_type
from project.server.main.pic_ror import get_rors, build_id_to_ror, resolve_ror as resolve_id_ror, ORGA_URL
from project.server.main.logger import get_logger

logger = get_logger(__name__)

OUTPUT_FILE = '/upw_data/scanr/orga_ref/rnsr_ror.json'


def normalize_rnsr(rnsr) -> Optional[str]:
    if not isinstance(rnsr, str):
        return None
    rnsr = rnsr.strip().upper()
    return rnsr if identifier_type(rnsr) == 'rnsr' else None


def get_rnsrs(struct: dict) -> set:
    """RNSR(s) d'une structure scanR : son id s'il s'agit d'un RNSR + les externalIds de type rnsr."""
    rnsrs = set()
    own = normalize_rnsr(struct.get("id"))
    if own:
        rnsrs.add(own)
    for ext in struct.get("externalIds") or []:
        if ext.get("type") == "rnsr":
            rnsr = normalize_rnsr(ext.get("id"))
            if rnsr:
                rnsrs.add(rnsr)
    return rnsrs


def get_active_institutions(struct: dict) -> list:
    """Identifiants scanR (paysage, ror, rnsr...) des tutelles non terminées."""
    today = str(datetime.today())[0:10]
    res = []
    for inst in struct.get("institutions") or []:
        if not isinstance(inst, dict) or not isinstance(inst.get("structure"), str):
            continue
        # on ne garde que les tutelles (cf projects.py)
        if inst.get("relationType") != "établissement tutelle":
            continue
        if isinstance(inst.get("endDate"), str) and inst["endDate"][0:10] < today:
            continue
        inst_id = inst["structure"].strip()
        if inst_id not in res:
            res.append(inst_id)
    return res


def build_rnsr_to_ror(all_struct: list) -> tuple:
    """
    Construit deux index RNSR -> set(ror) à partir des structures scanR :
    - by_scanr_id : structures dont l'id scanR est le RNSR (ROR issus d'IdRef)
    - by_external_id : structures (paysage, ...) portant le RNSR en externalId
    L'id scanR est prioritaire ; les externalIds servent de repli.
    Renvoie aussi les tutelles de chaque RNSR et l'ensemble des RNSR connus.
    """
    by_scanr_id = {}
    by_external_id = defaultdict(set)
    institutions = {}
    all_rnsrs = set()
    for struct in all_struct:
        rnsrs = get_rnsrs(struct)
        all_rnsrs |= rnsrs
        own = normalize_rnsr(struct.get("id"))
        if own:
            institutions[own] = get_active_institutions(struct)
        rors = get_rors(struct)
        if not rors:
            continue
        if own:
            by_scanr_id[own] = rors
        for rnsr in rnsrs:
            by_external_id[rnsr] |= rors
    return by_scanr_id, dict(by_external_id), institutions, all_rnsrs


def resolve_ror(rnsr: str, by_scanr_id: dict, by_external_id: dict) -> Optional[str]:
    rors = by_scanr_id.get(rnsr) or by_external_id.get(rnsr) or set()
    if len(rors) == 1:
        return next(iter(rors))
    if len(rors) > 1:
        logger.debug(f"Ambiguous ROR for {rnsr}: {sorted(rors)}")
    return None


def get_rnsr_ror_correspondance(all_struct: Optional[list] = None) -> dict:
    """
    Génère la correspondance RNSR -> ROR, au format
    {rnsr: {"rors": [ror, ...], "type": "self" | "institution" | "ambiguous" | "none"}}
    - self : ROR de la structure RNSR elle-même (un seul ROR)
    - institution : pas de ROR propre, ROR des tutelles (un ou plusieurs ROR)
    - ambiguous : plusieurs ROR propres, non tranché (rors = les candidats)
    - none : aucun ROR trouvé (rors vide)
    - all_struct : structures scanR, par défaut organizations-v2.jsonl.gz
    """
    if all_struct is None:
        all_struct = pd.read_json(ORGA_URL, lines=True).to_dict(orient='records')

    by_scanr_id, by_external_id, institutions, all_rnsrs = build_rnsr_to_ror(all_struct)
    # index id scanR / externalId -> ROR, pour résoudre les tutelles
    by_any_id, by_any_external_id = build_id_to_ror(all_struct)

    rnsr_ror = {}
    for rnsr in sorted(all_rnsrs):
        own_rors = by_scanr_id.get(rnsr) or by_external_id.get(rnsr)
        if own_rors:
            # la structure a son propre ROR
            ror = resolve_ror(rnsr, by_scanr_id, by_external_id)
            if ror:
                rnsr_ror[rnsr] = {'rors': [ror], 'type': 'self'}
            else:
                rnsr_ror[rnsr] = {'rors': sorted(own_rors), 'type': 'ambiguous'}
            continue
        institution_rors = set()
        for inst_id in institutions.get(rnsr, []):
            ror = resolve_id_ror(inst_id, by_any_id, by_any_external_id)
            if ror:
                institution_rors.add(ror)
        if institution_rors:
            rnsr_ror[rnsr] = {'rors': sorted(institution_rors), 'type': 'institution'}
        else:
            rnsr_ror[rnsr] = {'rors': [], 'type': 'none'}

    nb_by_type = Counter(e['type'] for e in rnsr_ror.values())
    logger.info(f"RNSR -> ROR : {len(rnsr_ror)} RNSR ({dict(nb_by_type)})")
    return rnsr_ror


def dump_rnsr_ror_correspondance(all_struct: Optional[list] = None) -> dict:
    rnsr_ror = get_rnsr_ror_correspondance(all_struct)
    with open(OUTPUT_FILE, 'w') as outfile:
        json.dump(rnsr_ror, outfile, indent=2)
    upload_object(container='scanr-data', source=OUTPUT_FILE, destination='production/rnsr_ror.json')
    return rnsr_ror
