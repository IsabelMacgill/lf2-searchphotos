import json
import os
import boto3
import time
import inflection
import logging

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

REGION = 'us-east-1'
HOST = 'search-photos-ir3dxedsasv6ddlx7gj7efuqn4.us-east-1.es.amazonaws.com'
INDEX = 'photo-labels'

## -- Get Slots & Search -- 

def get_slots_search(response):
    slots = response['sessionState']['intent']['slots']
    session_attributes = response['sessionState']['sessionAttributes'] if response['sessionState']['sessionAttributes'] is not None else {}
    
    search_term_1 = ""
    search_term_2 = ""
    if 'Query1' in slots: 
        search_term_1 = slots['Query1']['value']['originalValue']
        search_term_1 = inflection.singularize(search_term_1)
    if 'Query2' in slots: 
        if slots['Query2'] is not None: 
            search_term_2 = slots['Query2']['value']['originalValue']
            search_term_2 = inflection.singularize(search_term_2)
    
    logger.debug(search_term_1)
    logger.debug(search_term_2)
    
    q = {'size': 10000, 'query': {'multi_match': {'query': search_term_1}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    res1 = []
    for hit in hits:
        res1.append(hit['_source'])
        
    if search_term_2 == "":
        logger.debug('return res1 entered')
        return res1

    #If two search terms are defi
    q2 = {'size': 10000, 'query': {'multi_match': {'query': search_term_2}}}

    client2 = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res2 = client2.search(index=INDEX, body=q2)

    hits2 = res2['hits']['hits']
    res2 = []
    for hit in hits2:
        res2.append(hit['_source'])
    
    intersect_res = []
    for res in res1: 
        if res in res2: 
            intersect_res.append(res)
    
    
    if len(intersect_res) < 1: 
        logger.debug('entered intersect has few results')
        all_recs = []
        for res in res1: 
          all_recs.append(res)
        for res in res2: 
            if res not in all_recs: 
                all_recs.append(res)
        return all_recs
        
    return intersect_res

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)

## -- Get Signed URLS from S3 -- 
def get_urls(items_in):
    
    search_results = items_in
    formatted_search_res = []
    for item in search_results:
        key = item['objectKey']
        labels = item['labels']
        
        url = boto3.client('s3').generate_presigned_url(
        ClientMethod='get_object', 
        Params={'Bucket': 'photos-a2-cloud', 'Key': key},
        ExpiresIn=3600)
        
        
        formatted_item = {
            'url': url, 
            'labels': labels
        }
        formatted_search_res.append(formatted_item)
        
    return formatted_search_res

# # --- Main handler ---
def lambda_handler(event, context):
    client = boto3.client('lexv2-runtime')
    logger.debug('EVN')
    logger.debug(event)
    
    searching = ""
    if 'queryStringParameters' in event: 
        if 'q' in event['queryStringParameters']:
            searching = event['queryStringParameters']['q']
            
    searching = searching.replace("%20", " ")
    
    if searching == "":
        logger.debug('empt_search')
        # return res
        
    response = client.recognize_text(
        botId='RDV2J4EXJW',
        botAliasId='TSTALIASID',
        localeId='en_US',
        sessionId= '123',
        text= searching)
    
    logger.debug(response)
    
    search_results = get_slots_search(response)
    logger.debug(search_results)
    
    formatted_search_res = get_urls(search_results)
    
    
    res = {
    "statusCode": 200,
    "headers": { 
        'Content-Type': 'application/json',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*'},
    "body": json.dumps({'results': formatted_search_res})
    }
    return res