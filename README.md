# scanr-backend

This repo is used to build data behind scanR. The frontend / UI of scanR can be found here : https://github.com/dataesr/scanr-ui.

Except for the clinical-trials (see https://github.com/dataesr/bso-clinical-trials/) and for the patents (see https://github.com/dataesr/harvest-patstat/).

## Organizations

For the organizations, the entry point is project/server/main/meta_orga.py. Multiple data sources are collected and aggregated, by prioritization order: RNSR > Paysage > Siren > ROR. If an organization is found in multiple sources, no merge will be done, only the first accurrence will be kept.

As the Siren database is huge, a Siren or Siret will be added to scanR only if it is present in one of our other sources (ie. RNSR, Paysage or ROR).