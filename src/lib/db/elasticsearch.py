import os, re, logging
from elasticsearch import Elasticsearch, helpers

# Parse the auth and host from env:
bonsai = os.getenv("BONSAI_URL")
auth = re.search('https://(.*)@', bonsai).group(1).split(':')
host = bonsai.replace('https://%s:%s@' % (auth[0], auth[1]), '')

# optional port
match = re.search('(:\d+)', host)
if match:
    p = match.group(0)
    host = host.replace(p, '')
    port = int(p.split(':')[1])
else:
    port = 443

# Connect to cluster over SSL using auth for best security:
es_header = [{
    'host': host,
    'port': port,
    'use_ssl': True,
    'http_auth': (auth[0], auth[1])
}]

# Instantiate the new Elasticsearch connection:
elastic = Elasticsearch(es_header)


def prepare_payload(items):
    docs = []
    for item in items:
        if item is not None:
            docs.append(
                {
                    "_id": item['id'],
                    "_source": item
                }
            )
    return docs


def insert_data(payload):
    try:
        # make the bulk call using 'actions' and get a response
        response = helpers.bulk(elastic, payload, index='mercadolibre-items')
        logging.info("Payload successfully saved %s", response)
    except Exception as e:
        logging.error("Error inserting data to ElasticSearch %s", e)


