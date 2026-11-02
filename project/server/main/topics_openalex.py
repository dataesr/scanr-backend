import requests
import os
import time
from retry import retry
from project.server.main.logger import get_logger
logger = get_logger(__name__)

OPENALEX_TEXT_URL = 'https://api.openalex.org/text'
COLLECTION_NAME = 'project_topics_openalex'
# 0.01$ par appel : on plafonne le nombre d'appels par run pour tenir dans le budget journalier
MAX_CALLS = int(os.getenv('OPENALEX_MAX_CALLS', 19000))
nb_calls = 0
quota_reached = False

def get_from_mongo(pid, myclient):
    mydb = myclient['scanr']
    mycoll = mydb[COLLECTION_NAME]
    res = mycoll.find_one({'id': pid})
    if res:
        return res['cache']
    return

def save_to_mongo(pid, cache, myclient):
    mydb = myclient['scanr']
    mycoll = mydb[COLLECTION_NAME]
    mycoll.update_one({'id': pid}, {'$set': {'cache': cache}}, upsert=True)

def get_txt(project, field):
    elt = project.get(field)
    if isinstance(elt, dict):
        for lang in ['en', 'default', 'fr']:
            if isinstance(elt.get(lang), str):
                return elt[lang]
            if isinstance(elt.get(lang), list):
                return ' ; '.join(elt[lang])
    return ''

@retry(delay=30, tries=2, logger=logger)
def get_openalex_topics(project, myclient):
    global nb_calls, quota_reached
    pid = str(project['id'])
    pre_computed = get_from_mongo(pid, myclient)
    if isinstance(pre_computed, dict):
        return pre_computed
    if quota_reached or nb_calls >= MAX_CALLS:
        return
    title = get_txt(project, 'label')
    abstract = ' '.join([get_txt(project, 'description'), get_txt(project, 'keywords')]).strip()
    # l'API accepte entre 20 et 2000 caracteres (title + abstract)
    abstract = abstract[:max(0, 2000 - len(title))]
    if len(title) + len(abstract) < 20:
        return
    params = {'title': title[:2000], 'abstract': abstract}
    if os.getenv('OPENALEX_API_KEY'):
        params['api_key'] = os.getenv('OPENALEX_API_KEY')
    time.sleep(1) # rate limit 1 req/s
    nb_calls += 1
    r = requests.get(OPENALEX_TEXT_URL, params=params)
    if r.status_code in [402, 403, 429]:
        logger.debug(f'OpenAlex quota reached after {nb_calls} calls : {r.text}')
        quota_reached = True
        return
    try:
        assert r.status_code == 200
        res = r.json()
        # on garde aussi les reponses sans topic (texte non classable) pour ne pas repayer l'appel
        cache = {'openalex_topics': res.get('topics', []), 'openalex_primary_topic': res.get('primary_topic'), 'openalex_keywords': res.get('keywords', [])}
        save_to_mongo(pid, cache, myclient)
        return cache
    except:
        logger.debug(f'error in response from OpenAlex : {r.text}')
        logger.debug(f"input was {params.get('title')}")
