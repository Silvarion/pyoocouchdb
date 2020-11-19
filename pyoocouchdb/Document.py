import json
import logging
import uuid
from . import Core as f


# Document Class
class Document(object):
    # Initialization
    def __init__(self, database, doc_id=None, content={}):
        logger = logging.getLogger('Document::__init__')
        logger.debug('Initializing attributes')
        self.id = doc_id
        # self.content = json.dumps(content, ensure_ascii=False)
        self.content = content
        self.database = database
        self.revision = None
        # if "urlopener" in dir(database):
        #     self.urlopener = database.urlopener
        headers = {
            'Accept': 'application/json'
        }
        if self.id:
            self.url = f"{database.url}{self.id}"
            logger.debug('Looking for the document')
            resp = f.endpoint_api(self, endpoint='', headers=headers)
            logger.debug('Response from server: ' + json.dumps(resp, indent=2))
            logger.debug('Getting all content')
            if 'status' in resp.keys():
                if resp['status'] == 'error':
                    self.exists = False
            elif "error" in resp.keys():
                self.exists = False
            else:
                self.exists = True
                self.revision = resp['_rev']
                for key in resp.keys():
                    content[key] = resp[key]
        else:
            doc_id = uuid.uuid4().hex
            lookup = f.endpoint_api(
                object=self.database,
                endpoint=doc_id,
                headers=headers,
                method='GET'
            )
            if "id" in lookup.keys():
                while "error" not in lookup.keys():
                    doc_id = uuid.uuid4().hex
                    lookup = f.endpoint_api(
                        object=self.database,
                        endpoint=doc_id,
                        headers=headers,
                        method='GET'
                    )
            self.id = doc_id
            self.exists = False
            self.url = f"{self.database.url}{self.id}"
            self.content['_id'] = doc_id

    # Returns a string description
    def __str__(self):
        dict_json = {}
        dict_json["_id"] = self.id
        dict_json["database"] = self.database.name
        dict_json["content"] = self.content
        dict_json["revision"] = self.revision
        dict_json["url"] = self.url
        dict_json["exists"] = self.exists
        
        return json.dumps(dict_json,indent=4)

    # Checks and updates existence of a document
    def is_there(self):
        logger = logging.getLogger('Document::create')
        logger.debug("Looking for the document")
        response = f.endpoint_api(object = self, endpoint = "")
        if "error" in response.keys():
            self.exists = False
            return False
        elif "_id" in response.keys():
            self.exists = True
            return True

    # Creates a non-existent document
    def create(self):
        logger = logging.getLogger('Document::create')
        logger.debug(f"Cheking if document exists at {self.url}")
        self.is_there()
        if self.exists:
            logger.warning('Document exists already, no need to create it')
            return {"status": "error", "errcode": "400", "errmsg": "Document already exists!"}
        else:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            response = f.endpoint_api(self, endpoint='', headers=headers, json_data=self.content, method='PUT')
            if "rev" in response.keys():
                self.revision = response["rev"]
                self.exists = True
            # if response["ok"]:
            logger.debug(f"{json.dumps(response,indent=2)}")
            return response
    
    # Set the revision of the document
    def current_revision(self):
        logger = logging.getLogger('Document::current_revision')
        logger.debug("Getting revision set")
        data = self.database.find_by_id(doc_id = self.id)
        self.revision = data.revision
        self.content = data.content

    # Updates existing document
    def update(self):
        logger = logging.getLogger('Document::update')
        logger.debug("Saving updated content")
        updated_content = json.loads(json.dumps(self.content))
        logger.debug(f"Saved Data: {json.dumps(updated_content,indent=2)}")
        logger.debug(f"Cheking if document exists at {self.url}")
        if self.is_there():
            self.current_revision()
        if self.exists:
            logger.debug(f"Document found!")
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                'If-Match': self.revision
            }
            logger.debug(f"Saved Data before endpoint call: {json.dumps(updated_content,indent=2)}")
            response = f.endpoint_api(self, endpoint='', headers=headers, json_data=updated_content, method='PUT')
            self.current_revision()
            logger.debug(f"{json.dumps(response,indent=2)}")
            return response
        else:
            logger.warning('Document not found')
            return {"status": "error", "errcode": "400", "errmsg": "Document already exists!"}

    # Deletes existing document
    def delete(self):
        logger = logging.getLogger('Document::create')
        logger.debug('Cheking if document exists')
        self.current_revision()
        if self.is_there():
            if not self.exists:
                logger.warning('Document does not exist, no need to delete it')
                return {"status": "error", "errcode": "400", "errmsg": "Document doesn't exist!"}
            else:
                headers = {
                    'Accept': 'application/json',
                    'If-Match': self.revision
                }
                return f.endpoint_api(self, endpoint='', headers=headers, method='DELETE')
