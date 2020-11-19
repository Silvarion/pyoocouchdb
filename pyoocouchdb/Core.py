########
#
# Filename: couchdblib.py
# Author: Jesus Alejandro Sanchez Davila
# Name: couchdblib
#
# Description: Library for CouchDB v2.x.x
#
##########

# Imports
import http.client
from inspect import currentframe, getframeinfo
import json
import logging
from urllib3 import make_headers
import uuid
from .Database import Database
from .Document import Document
from .Node import Node
from .Server import Server
try:
    import requests
    requests_module = True
except ModuleNotFoundError as err:
    print("WARNING::Import Section: Module 'requests' not found, falling back to 'http.client'")
    requests_module = False


def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno

# API Endpoint Interaction
def endpoint_api(object, endpoint, headers={}, data={}, json_data={}, method='GET', admin=False, compatibility=False):
    logger = logging.getLogger('endpoint_api')
    endpoint_url = f"{object.url}{endpoint}"
    logger.debug(f"[{get_linenumber()}] Endpoint URL: {endpoint_url}")
    logger.debug(f"[{get_linenumber()}] Method: {method}")
    logger.debug(f"[{get_linenumber()}] Headers: {headers}")
    if len(headers.keys()) == 0:
        logger.debug(f"[{get_linenumber()}] Empty header detected, using default")
        default_header = True
    else:
        default_header = False
        logger.debug(f"[{get_linenumber()}] Headers: {json.dumps(headers, indent=2)}")
    if data is None:
        logger.debug(f"[{get_linenumber()}] Empty data detected")
    else:
        logger.debug(f"[{get_linenumber()}] Data: {data}")
    if json_data is None:
        logger.debug(f"[{get_linenumber()}] Empty json detected")
    else:
        logger.debug(f"[{get_linenumber()}] Data: {json.dumps(json_data, indent=2)}")

    # Set credentials
    if type(object) is Server:
        creds = (object.username,object.password)
    elif type(object) is Database:
        creds = (object.server.username,object.server.password)
    elif type(object) is Node:
        creds = (object.server.username,object.server.password)
    elif type(object) is Document:
        creds = (object.database.server.username,object.database.server.password)
    logger.debug(f"[{get_linenumber()}] Checking which module to use")
    if requests_module: ## When the requests module is found
        try:
            if admin:
                logger.debug(f"[{get_linenumber()}] Using admin mode")
                if type(object) is Server:
                    headers["Host"] = object.admin_host
                    headers["Referer"] = f"http://{object.admin_host}"
                    headers["Referer"] = object.admin_url
                elif type(object) is Database:
                    headers["Host"] = object.server.admin_host
                    headers["Referer"] = f"http://{object.server.admin_host}"
                    headers["Referer"] = object.server.admin_url
                elif type(object) is Document:
                    headers["Host"] = object.database.server.admin_host
                    headers["Referer"] = f"http://{object.database.server.admin_host}"
                    headers["Referer"] = object.database.server.admin_url
            else:
                if type(object) is Server:
                    headers["Host"] = object.couchdb_host
                    headers["Referer"] = f"http://{object.couchdb_host}"
                    headers["Referer"] = object.url
                elif type(object) is Database:
                    headers["Host"] = object.server.couchdb_host
                    headers["Referer"] = f"http://{object.server.couchdb_host}"
                    headers["Referer"] = object.server.url
                elif type(object) is Document:
                    headers["Host"] = object.database.server.couchdb_host
                    headers["Referer"] = f"http://{object.database.server.couchdb_host}"
                    # headers["Referer"] = object.database.server.url
            if default_header:
                headers["accept"] = "application/json"
            logger.debug(f"[{get_linenumber()}] Final header:\n{headers}")
            if method.upper() == 'GET':
                response = requests.get(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'PUT':
                response = requests.put(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'POST':
                response = requests.post(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'DELETE':
                response = requests.delete(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            else:
                response = requests.request(method=method.upper(), url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
        except requests.HTTPError as he:
            logger.warning('HTTPError while trying the connection')
            logger.debug(he)
            response = {
                'status': 'error',
                'headers': {}
            }
        except Exception as e:
            logger.critical(e.__str__())
            response = {
                'status': 'error',
                'fullerror': e.__str__()
            }
        finally:            
            if requests_module:
                logger.debug(f"Crude response >>> \n{response}")  
                if "json" in dir(response):
                    return response.json()
                else:
                    return response.raw.read().decode()
            else:
                return response
    else: ## Falback to http.client module
        # Split Endpoint URL into base + path
        host_end = endpoint_url.find("/",7)
        url_base = endpoint_url[:host_end].split("/")[2]
        url_path = endpoint_url[host_end:]
        logger.debug(f"[{get_linenumber()}] URL Base: {url_base}\nURL Path: {url_path}")
        try:
            logger.debug(f"[{get_linenumber()}] Preparing request using http.client")
            http_conn = http.client.HTTPConnection(host=url_base)
            # http_conn.set_debuglevel(100)
            headers_auth = make_headers(basic_auth=f"{creds[0]}:{creds[1]}")
            if admin:
                logger.debug(f"[{get_linenumber()}] Using admin mode")
                if type(object) is Server:
                    headers["Host"]=object.admin_host
                    headers["Referer"]=f"http://{object.admin_host}"                    
                elif type(object) is Database:
                    headers["Host"]=object.server.admin_host
                    headers["Referer"]=f"http://{object.server.admin_host}"                    
                elif type(object) is Document:
                    headers["Host"]=object.database.server.admin_host
                    headers["Referer"]=f"http://{object.database.server.admin_host}"                    
            else:
                if type(object) is Server:
                    headers["Host"]=object.couchdb_host
                    headers["Referer"]=f"http://{object.couchdb_host}"                    
                elif type(object) is Database:
                    headers["Host"]=object.server.couchdb_host
                    headers["Referer"]=f"http://{object.server.couchdb_host}"                    
                elif type(object) is Document:
                    headers["Host"]=object.database.server.couchdb_host
                    headers["Referer"]=f"http://{object.database.server.couchdb_host}"                    
            if default_header:
                headers["accept"] = "application/json"
            headers["authorization"] = headers_auth["authorization"]
            logger.debug(f"[{get_linenumber()}] Final header:\n{headers}")
            logger.debug(f"[{get_linenumber()}] Connection attempt: {http_conn.connect()}")
            if data:
                http_conn.request(method=method.upper(), url=url_path, body=data, headers=headers)    
            elif json_data:
                http_conn.request(method=method.upper(), url=url_path, body=bytes(json.dumps(json_data), 'utf-8'), headers=headers)
            else:
                http_conn.request(method=method.upper(), url=url_path, headers=headers)
            response = json.loads(http_conn.getresponse().read().decode())
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            response = {
                "status": f"{response.status}",
                "reason": f"{response.reason}",
                "content": str(he)
            }
        except Exception as e:
            logger.critical(e.__str__())
            response = {
                'status': 'error',
                'fullerror': e.__str__()
            }
        finally:
            logger.debug(response)
            if requests_module:
                if response.json():
                    return response.json()
                else:
                    return response.raw.read().decode()
            else:
                return response





