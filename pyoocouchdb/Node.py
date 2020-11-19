import logging
from . import Core as f

# Node Class
class Node(object):
    # Initialization
    def __init__(self, server, name):
        logger = logging.getLogger('Node::__init__')
        logger.info('Checking node name against cluster nodes list')
        self.name = name
        self.url = server.url + '_node/' + name + '/'
        # if server.urlopener:
        #     self.urlopener = server.urlopener
        self.server = server

    # Node Config
    def config(self, section=None, key=None, data=None, method='GET'):
        logger = logging.getLogger('Node::config')
        logger.debug("Building endpoint")
        endpoint = '_config'
        if section:
            endpoint = endpoint + '/' + section
            if key:
                endpoint = endpoint + '/' + key
            headers = {
                'Accept': 'application/json'
            }
        return f.endpoint_api(self, endpoint=endpoint, headers=headers, data=data, method=method.upper())

    # Node Stats
    def stats(self):
        logger = logging.getLogger('Node::stats')
        logger.debug("Building endpoint")
        endpoint = '_stats'
        return f.endpoint_api(self, endpoint=endpoint)

    # Node System
    def system(self):
        logger = logging.getLogger('Node::system')
        logger.debug("Building endpoint")
        endpoint = '_system'
        return f.endpoint_api(self, endpoint=endpoint)
