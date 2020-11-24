

class ClientConfig:

    def __init__(self, zeppelin_rest_url, query_interval = 1, knox_sso_url = None):
        self.zeppelin_rest_url = zeppelin_rest_url
        self.query_interval = query_interval
        self.knox_sso_url = knox_sso_url

    def get_zeppelin_rest_url(self):
        return self.zeppelin_rest_url

    def get_query_interval(self):
        return self.query_interval


