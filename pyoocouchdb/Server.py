import logging
import json
import http.client
from . import Core as f
from .Database import Database
from .Document import Document
from .Node import Node

MASTER_LOG_LEVEL = logging.DEBUG

# Server Class - As in a CouchDB Instance/Cluster
class Server(object):
    # Initialization
    def __init__(self, hostname, port=5984, admin_port=5986, username="", password="", compatibility=False, log_level=MASTER_LOG_LEVEL):
        logger = logging.getLogger('Server::__init__')
        logging.basicConfig(level=log_level)
        logger.debug('Initializing static variables')
        self.hostname = hostname
        self.admin_port = admin_port
        self.port = str(port)
        self.username = username
        self.password = password
        self.couchdb_host = f"{self.hostname}:{self.port}"
        self.admin_host = f"{self.hostname}:{self.admin_port}"
        self.url = f'http://{self.couchdb_host}/'
        self.admin_url = f'http://{self.admin_host}/'
        self.compatible=compatibility
        try:
            response = f.endpoint_api(object=self, endpoint="")
            logger.debug(f"Response from connection: {response}")
            if "error" not in response.keys():
                logger.debug(f"Response:\n{json.dumps(response,indent=2)}")
                if "version" in response.keys():
                    self.version = response['version']
                if "features" in response.keys():
                    self.features = response['features']
                if "vendor" in response.keys():
                    self.vendor = response['vendor']['name']
                if "all_nodes" in response.keys():
                    self.all_nodes = self.membership()['all_nodes']
                if "version" in dir(self):
                    logger.info(f'Connected to CouchDB v{self.version} instance on {self.hostname}')
                else:
                    logger.info(f'Connected to CouchDB instance on {self.hostname}')
            else:
                logger.info(f'Error connecting to CouchDB:\n{json.dumps(response,indent=2)}')
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            response = {
                "status": "error",
                "content": str(he)
            }

    # Refresh connection
    def refresh_connection(self):
        self.__init__(hostname=self.hostname, port=self.port, admin_port=self.admin_port, username=self.username, password=self.password)

    # API Endpoint Interaction
    def endpoint(self, endpoint, headers={}, data=None, json_data=None, method='GET', admin=False):
        logger = logging.getLogger('Server::endpoint')
        logger.debug('Calling main endpoint function')
        # self.refresh_connection()
        return f.endpoint_api(self, endpoint=endpoint, headers=headers, data=data, json_data=json_data, method=method,admin=admin,compatibility=self.compatible)

    # Active tasks
    def active_tasks(self):
        logger = logging.getLogger('Server::active_tasks')
        logger.debug('Querying /_active_tasks')
        # self.refresh_connection()
        return self.endpoint(endpoint='_active_tasks')

    # All DBs
    def all_dbs(self):
        logger = logging.getLogger('Server::all_dbs')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_all_dbs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_all_dbs', headers=headers)

    # DBs Info
    def dbs_info(self, dbs_list = None):
        logger = logging.getLogger('Server::dbs_info')
        if dbs_list is None:
            logger.debug("No dbs_list provided")
            dbs = self.all_dbs()
            logger.debug(f"List from the server itself {dbs}")
        elif type(dbs_list) is str:
            logger.debug(f"String list provided")
            dbs = dbs_list.split(',')
            if len(dbs) == 1:
                dbs = dbs_list.split(' ')
            logger.debug(f"DBs list: {dbs}")
        else:
            dbs = dbs_list
            logger.debug(f"DBs list: {dbs}")
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        json_data = {
            'keys': dbs
        }
        logger.debug('Querying _dbs_info')
        # self.refresh_connection()
        return self.endpoint(endpoint='_dbs_info', headers=headers, json_data=json_data, method='POST')

    # Cluster Setup Status
    def cluster_setup_status(self, username, password):
        logger = logging.getLogger('Server::cluster_setup_status')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_cluster_setup')
        # self.refresh_connection()
        return self.endpoint(endpoint='_cluster_setup', headers=headers)

    def setup_cluster(self, username=None, password=None, seed_list = []):
        logger = logging.getLogger('Server::setup_cluster')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Checking cluster setup status')
        # current = self.cluster_setup_status(username,password)
        results = {
            "status": "200",
            "nodes": {}
        }
        gc_db = Database(server = self, name = "_global_changes")
        if not gc_db.exists:
            rsp = gc_db.create()
            print(rsp)
            users_db = Database(server = self, name = "_users")
            if not users_db.exists:
                rsp = users_db.create()
                print(rsp)
            repl_db = Database(server = self, name = "_replicator")
            if not repl_db.exists:
                rsp = repl_db.create()
                print(rsp)
            json_data = {
                "action": "enable_cluster",
                "bind_address": "0.0.0.0",
                "username": username,
                "password": password,
                "node_count": f"{len(seed_list)}"
            }
            print(json.dumps(json_data,indent=2))
            setup_state = self.endpoint(endpoint="_cluster_setup", headers=headers, json_data=json_data,method="POST")
            logger.debug(setup_state)
            for node in seed_list:
                if type(node) is Node:
                    json_data = {
                        "action": "add_node",
                        "host": node.name,
                        "port": self.port,
                        "username": username,
                        "password": password,
                    }
                elif type(node) is str:
                    json_data = {
                        "action": "add_node",
                        "host": node,
                        "port": 5984,
                        "username": username,
                        "password": password,
                    }
                results["nodes"][node] = self.endpoint(endpoint="", headers=headers, json_data=json_data, method="POST")
                logger.debug(results["nodes"][node])
            ## Finish the cluster setup
            setup_state = self.endpoint(endpoint="_cluster_setup", headers=headers, json_data={ "action": "finish_cluster"},method="POST")
        else:
            for node in seed_list:
                results["nodes"][node] = self.add_node(node.name.split('@')[1])

    def create_initial_dbs(self):
        db = Database(server = self, name = "_users")
        if not db.exists:
            db.create()
        db = Database(server = self, name = "_replicator")
        if not db.exists:
            db.create()
        db = Database(server = self, name = "_global_changes")
        if not db.exists:
            db.create()

    # Server User/Admin Addition/Removal
    def add_user(self, username, password, roles = []):
        user_db = Database(server=self, name="_users")
        user_doc = Document(database=user_db,doc_id=f"org.couchdb.user:{username}")
        if not user_doc.exists:
            user_doc.content = {
                "name": username,
                "password": password,
                "roles": roles,
                "type": "user"
            }
            user_doc.create()

    def delete_user(self, username):
        logger = logging.getLogger("Server::delete_user")
        users = Database(server=self, name="_users")
        to_drop = Document(database=users,doc_id=f"org.couchdb.user:{username}")
        if to_drop.exists:
            to_drop.delete()
        else:
            logger.error("User does not exist. Nothing to do")

    # Node Addition/Removal
    def add_node(self, node):
        logger = logging.getLogger('Server::add_nodes')
        # self.refresh_connection()
        result = self.membership()
        if "all_nodes" in result.keys():            
            if node.name in result['all_nodes']:
                logger.warning(f"{node.name} is already a member of the cluster")
                response = {
                    "status": "error",
                    "errcode": "400",
                    "body": {
                        "node": node.name,
                        "message": "Node already registered"
                    }
                }
            else:
                response = self.endpoint(endpoint = f"_node/_local/_nodes/{node.name}", method="PUT", admin=True, data={})
        else:
            response = {
                "status": "error",
                "errcode": "500",
                "body": {
                    "node": node.name,
                    "message": "Error trying to get membership"
                }
            }
        return response

    def remove_node(self, node):
        logger = logging.getLogger('Server::add_nodes')
        result = self.membership()
        if "all_nodes" in result.keys():            
            if node.name in result['all_nodes']:
                logger.warning(f"{node.name} is visible to the cluster")
            if "cluster_nodes" in result.keys():
                if node.name in result['cluster_nodes']:
                    # Get Node document revision
                    response = self.endpoint(endpoint = f"_node/_local/_nodes/{node.name}", method="GET", admin=True, data={})
                    endpoint = f"_node/_local/_nodes/{node.name}?rev={response['_rev']}"
                    response = self.endpoint(endpoint=endpoint, method="DELETE", admin=True, data={})
        else:
            response = {
                "status": "error",
                "errcode": "500",
                "body": {
                    "node": node.name,
                    "message": "Error trying to get membership"
                }
            }
        return response

    # Sync all 
    def sync_all_shards(self):
        logger = logging.getLogger("Server::sync_all_shards")
        result = {
            "processed": 0,
            "rows": {}
        }
        db_list = self.all_dbs()
        logger.debug(f"dblist type is {type(db_list)}")
        if type(db_list) is list:
            for item in db_list:
                logger.info(f"Syncing shards for {item}")
                result["processed"] += 1
                db = Database(server=self, name=item)
                result["rows"][item] = db.sync_shards()
            return result
        else:
            logger.error("Invalid List!!")
            return db_list

    # Compact all
    def compact_all(self):
        result = {
            "processed": 0,
            "rows": {}
        }
        db_list = self.all_dbs()
        if "error" not in db_list.keys():
            for item in db_list:
                result["processed"] += 1
                db = Database(server=self, name=item)
                result["rows"][item] = db.compact()
            return result
        else:
            return db_list

    # DB Updates
    def db_updates(self):
        logger = logging.getLogger('Server::db_updates')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_db_updates')
        # self.refresh_connection()
        return self.endpoint(endpoint='_db_updates', headers=headers)

    # Cluster Membership
    def membership(self):
        logger = logging.getLogger('Server::membership')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_membership')
        # self.refresh_connection()
        return f.endpoint_api(self, endpoint='_membership', headers=headers)

    # Scheduler Jobs
    def scheduler_jobs(self):
        logger = logging.getLogger('Server::scheduler_jobs')
        headers = {
            'Accept': 'application/json'
        }
        logger.debug('Querying /_scheduler/jobs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_scheduler/jobs', headers=headers)

    # Scheduler Docs
    def scheduler_docs(self):
        logger = logging.getLogger('Server::scheduler_docs')
        headers = {
            'Accept': 'application/json'
        }
        logger.debug('Querying /_scheduler/docs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_scheduler/docs', headers=headers)

    # Up
    def up(self):
        logger = logging.getLogger('Server::up')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_up')
        # self.refresh_connection()
        return self.endpoint(endpoint='_up', headers=headers)

    # UUIDs
    def uuids(self):
        logger = logging.getLogger('Server::uuids')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_uuids')
        # self.refresh_connection()
        return self.endpoint(endpoint='_uuids', headers=headers)
