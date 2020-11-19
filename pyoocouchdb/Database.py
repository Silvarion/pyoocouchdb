import logging
import http.client
import json
import uuid
from . import Core as f
from .Document import Document

# Database Class
class Database(object):
    # Initialization
    def __init__(self, server, name):
        logger = logging.getLogger('Database::__init__')
        logger.debug('Initializing Database object')
        self.server = server
        self.name = name
        self.url = f"{self.server.url}{name}/"
        # if "urlopener" in dir(server):
        #     self.urlopener = server.urlopener
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        try:
            logger.debug('Looking for the database')
            resp = f.endpoint_api(self, endpoint='', headers=headers)
            logger.debug('Response from server: ' + json.dumps(resp, indent=2))
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            resp = {
                "status": "error",
                "content": str(he)
            }
        except Exception as ue:
            resp = {
                'status': 'error',
                'content': ue
            }
        logger.debug("Finishing initialization")
        if 'db_name' in resp.keys():
            logger.debug("Database found!")
            self.name = resp['db_name']
            self.exists = True
        else:
            logger.debug("Database NOT found!")
            self.name = name
            self.exists = False

    def __str__(self):
        return self.url

    # Creates a non-existent database
    def create(self):
        logger = logging.getLogger('Database::create')
        logger.debug('Cheking if DB exists')
        if self.exists:
            logger.warning('Database exists already, no need to create it')
        else:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            response = f.endpoint_api(self, endpoint='', headers=headers, method='PUT')
            if "ok" in response.keys():
                if response["ok"]:
                    self.exists = True
            return response

    # Deletes existing database
    def delete(self):
        logger = logging.getLogger('Database::delete')
        logger.debug('Cheking if DB exists')
        if not self.exists:
            logger.warning('Database does not exist, no need to delete it')
        else:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            response = f.endpoint_api(self, endpoint='', headers=headers, method='DELETE')
            if "ok" in response.keys():
                if response["ok"]:
                    self.exists = False
            return response

    def all_docs(self):
        return f.endpoint_api(self, endpoint="_all_docs")

    def delete_all_docs(self):
        result = self.all_docs()
        if "rows" in result.keys():
            for row in result["rows"]:
                print(row)
                doc = Document(database = self, doc_id = row["id"])
                doc.delete()
        self.purge_all()

    # Returns the change log of the DB
    def changes(self):
        logger = logging.getLogger('Database::changes')
        logger.debug('Building endpoint')
        return f.endpoint_api(self, endpoint='_changes')

    # Security Data
    def get_security_data(self):
        logger = logging.getLogger("Database::set_security_data")
        resp = self.server.endpoint(endpoint=f"{self.name}/_security", method="GET")
        logger.debug(json.dumps(resp,indent=2))
        return(resp)

    def set_security_data(self, definition):
        logger = logging.getLogger("Database::set_security_data")
        resp = self.server.endpoint(endpoint=f"{self.name}/_security", json_data=definition, method="PUT")
        logger.info(json.dumps(resp,indent=2))
    
    def add_admin_user(self, username):
        logger = logging.getLogger("Database::add_admin_user")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "names" not in sec_data["admins"].keys():
            sec_data["admins"]["names"] = []
        if username not in sec_data["admins"]["names"]:
            sec_data["admins"]["names"].append(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_admin_user(self, username):
        logger = logging.getLogger("Database::remove_admin_user")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "names" not in sec_data["admins"].keys():
            sec_data["admins"]["names"] = []
        if username in sec_data["admins"]["names"]:
            sec_data["admins"]["names"].remove(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{username} is not a member of the admins group")

    def add_admin_role(self, role_name):
        logger = logging.getLogger("Database::add_admin_role")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "roles" not in sec_data["admins"].keys():
            sec_data["admins"]["roles"] = []
        if role_name not in sec_data["admins"]["roles"]:
            sec_data["admins"]["roles"].append(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_admin_role(self, role_name):
        logger = logging.getLogger("Database::remove_admin_role")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "roles" not in sec_data["admins"].keys():
            sec_data["admins"]["roles"] = []
        if role_name in sec_data["admins"]["roles"]:
            sec_data["admins"]["roles"].remove(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{role_name} is not a member of the admins group")

    def add_member_user(self, username):
        logger = logging.getLogger("Database::add_member_user")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "names" not in sec_data["members"].keys():
            sec_data["members"]["names"] = []
        if username not in sec_data["members"]["names"]:
            sec_data["members"]["names"].append(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_member_user(self, username):
        logger = logging.getLogger("Database::remove_member_user")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "names" not in sec_data["members"].keys():
            sec_data["members"]["names"] = []
        if username in sec_data["members"]["names"]:
            sec_data["members"]["names"].remove(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{username} is not a member of the members group")

    def add_member_role(self, role_name):
        logger = logging.getLogger("Database::add_member_role")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "roles" not in sec_data["members"].keys():
            sec_data["members"]["roles"] = []
        if role_name not in sec_data["members"]["roles"]:
            sec_data["members"]["roles"].append(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_member_role(self, role_name):
        logger = logging.getLogger("Database::remove_member_role")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "roles" not in sec_data["members"].keys():
            sec_data["members"]["roles"] = []
        if role_name in sec_data["members"]["roles"]:
            sec_data["members"]["roles"].remove(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{role_name} is not part of the members group")

    # Find document by ID
    def find_by_id(self, doc_id):
        logger = logging.getLogger('Database::find_by_id')
        logger.debug('Looking for the doc')
        return Document(self, doc_id=doc_id)

    # Find using the JSON query syntax
    def find(self, query = None):
        logger = logging.getLogger('Database::find')
        logger.debug('Looking for docs matching criteria')
        # self.server.refresh_connection()
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        resp = f.endpoint_api(self, endpoint='_find',
                            headers=headers, json_data=query, method='POST')
        return resp

    # Bulk Insert
    def bulk_create(self, docs):
        logger = logging.getLogger('Database::create_index')
        logger.debug('Creating headers')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        endpoint = '_bulk_docs'
        return f.endpoint_api(self, endpoint=endpoint, headers=headers, data=docs, method='POST')

    # Create Index on a Database
    def create_index(self, definition):
        logger = logging.getLogger('Database::create_index')
        logger.debug('Creating headers')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Checking index definition')
        if 'index' in definition.keys():
            if 'fields' in definition.index.keys():
                if 'name' in definition.keys():
                    if 'type' in definition.keys():
                        if definition['type'] in ['json', 'text']:
                            logger.debug('Index definition looks good!')
                        else:
                            logger.debug(
                                'Wrong type in index definition, must be either json or text')
                    else:
                        logger.debug('Missing type in index definition')
                else:
                    logger.debug('Missing name in index definition')
            else:
                logger.debug('Missing fields in index definition')
        else:
            logger.debug('Missing index in index definition')
        logger.debug('Trying Index Creation')
        return f.endpoint_api(self, endpoint='_index/', headers=headers, data=definition, method='POST')

    # Create View in Database
    def create_view(self, name, definition):
        logger = logging.getLogger('Database::create_view')
        logger.debug('Creating headers')
        logger.debug('Checking ')
        logger.debug('Trying View Creation')
        view = Document(
            database=self,
            doc_id=f"_design/{name}",
            content=definition
        )
        return(view.create())

    # Purge deleted docs
    def purge_all(self):
        logger = logging.getLogger('Database::purge_all')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        data = {
            "selector": {
                "deleted": True
            }
        }
        logger.debug("Finding deleted documents")
        changes = f.endpoint_api(
            object=self, endpoint='_changes', headers=headers, data=data, method='POST')
        logger.debug('Changes: ' + json.dumps(changes, indent=2))
        purge_id = uuid.uuid4().hex
        logger.debug('purge_id: ' + purge_id)
        data = {
            purge_id: []
        }
        latest_change = changes['last_seq']
        for doc in changes['results']:
            if "deleted" in doc.keys():
                if doc['deleted'] and doc['seq'] == latest_change:
                    for change in doc['changes']:
                        data[purge_id].append(change['rev'])
        purge = f.endpoint_api(object=self, endpoint='_purge',
                             headers=headers, data=data, method='POST')
        return purge

    # Sync Shards
    def sync_shards(self):
        return f.endpoint_api(object=self, endpoint="_sync_shards", method="POST")

    # Compact DB
    def compact(self):
        return f.endpoint_api(object=self, endpoint="_sync_shards", method="POST")
