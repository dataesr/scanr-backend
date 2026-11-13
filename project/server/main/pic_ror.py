import os
import re
import json
from collections import defaultdict
from typing import Optional

import requests
import pandas as pd

from project.server.main.s3 import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

ROR_REGEX = re.compile(r"^0[a-z0-9]{6}[0-9]{2}$")
ORGA_URL = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations-v2.jsonl.gz'
# identifiants scanR utilisables comme clé pour retrouver une structure
ID_TYPES = ["ror", "paysage", "rnsr", "siren", "siret", "grid"]
OUTPUT_FILE = '/upw_data/scanr/orga_ref/pic_ror.json'


def normalize_ror(ror) -> Optional[str]:
    if not isinstance(ror, str):
        return None
    ror = ror.strip().lower().replace("https://ror.org/", "").replace("ror.org/", "")
    return ror if ROR_REGEX.match(ror) else None


def get_rors(struct: dict) -> set:
    """ROR(s) d'une structure scanR : son id s'il s'agit d'un ROR + les externalIds de type ror."""
    rors = set()
    own = normalize_ror(struct.get("id"))
    if own:
        rors.add(own)
    for ext in struct.get("externalIds") or []:
        if ext.get("type") == "ror":
            ror = normalize_ror(ext.get("id"))
            if ror:
                rors.add(ror)
    return rors


def get_pic_correspondance() -> dict:
    """PIC -> id scanR (paysage / rnsr / siren / ror), depuis la table Grist (cf harvest-grants eu.py)."""
    GRIST_TOKEN = os.getenv('GRIST_TOKEN')
    DOC_ID = "s8u4VDyE9RoTp38S22gQE8"
    TABLE_ID = "From_pic_to_id"
    URL = f"https://grist.numerique.gouv.fr/api/docs/{DOC_ID}/tables/{TABLE_ID}/records"
    HEADERS = {"Authorization": f"Bearer {GRIST_TOKEN}"}
    r = requests.get(URL, headers=HEADERS, timeout=60)
    r.raise_for_status()

    pic_map = {}
    for record in r.json().get('records', []):
        fields = record.get('fields', {})
        pic = fields.get('generalPic')
        ref = fields.get('from_id_to_ref')
        if isinstance(pic, str) and isinstance(ref, str):
            pic_map[pic] = ref.strip()
    return pic_map


def build_id_to_ror(all_struct: list) -> tuple:
    """
    Construit deux index vers les ROR à partir des structures scanR :
    - by_scanr_id : id scanR (siren / rnsr / paysage / ror) -> set(ror)
    - by_external_id : identifiant externe (paysage, rnsr, siren, siret, grid, ror) -> set(ror)
    L'id scanR est prioritaire ; les externalIds servent de repli
    (un même siren / ror peut être porté par plusieurs structures).
    """
    by_scanr_id = {}
    by_external_id = defaultdict(set)
    for struct in all_struct:
        rors = get_rors(struct)
        if not rors:
            continue
        by_scanr_id[struct["id"]] = rors
        for ext in struct.get("externalIds") or []:
            if ext.get("type") in ID_TYPES and isinstance(ext.get("id"), str):
                by_external_id[ext["id"].strip()] |= rors
    return by_scanr_id, dict(by_external_id)


def resolve_ror(identifier: str, by_scanr_id: dict, by_external_id: dict) -> Optional[str]:
    identifier = identifier.strip()
    direct = normalize_ror(identifier)
    if direct:
        return direct
    rors = by_scanr_id.get(identifier) or by_external_id.get(identifier) or set()
    if len(rors) == 1:
        return next(iter(rors))
    if len(rors) > 1:
        logger.debug(f"Ambiguous ROR for {identifier}: {sorted(rors)}")
    return None


def get_pic_ror_correspondance(pic_map: Optional[dict] = None, all_struct: Optional[list] = None) -> dict:
    """
    Génère la correspondance PIC -> ROR.
    - pic_map : PIC -> id scanR (paysage / rnsr / siren / ror), par défaut la table Grist
    - all_struct : structures scanR, par défaut organizations-v2.jsonl.gz
    """
    if pic_map is None:
        pic_map = get_pic_correspondance()
    if all_struct is None:
        all_struct = pd.read_json(ORGA_URL, lines=True).to_dict(orient='records')

    by_scanr_id, by_external_id = build_id_to_ror(all_struct)

    pic_ror = {}
    unresolved = []
    for pic, identifier in pic_map.items():
        ror = resolve_ror(identifier, by_scanr_id, by_external_id)
        if ror:
            pic_ror[str(pic)] = ror
        else:
            unresolved.append((pic, identifier))

    logger.info(f"PIC -> ROR : {len(pic_ror)} / {len(pic_map)} PIC résolus ({len(unresolved)} sans ROR)")
    return pic_ror


def dump_pic_ror_correspondance(all_struct: Optional[list] = None) -> dict:
    pic_ror = get_pic_ror_correspondance(all_struct=all_struct)
    with open(OUTPUT_FILE, 'w') as outfile:
        json.dump(pic_ror, outfile, indent=2)
    upload_object(container='scanr-data', source=OUTPUT_FILE, destination='production/pic_ror.json')
    return pic_ror
