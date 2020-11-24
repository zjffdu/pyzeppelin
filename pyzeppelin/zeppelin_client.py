
import requests
from pyzeppelin.config import ClientConfig
from pyzeppelin.result import NoteResult
from pyzeppelin.result import ParagraphResult
import time
import logging


class SessionInfo:

    def __init__(self, resp_json):
        self.session_id = None
        self.note_id = None
        self.interpreter = None
        self.state = None
        self.weburl = None
        self.start_time = None

        if "sessionId" in resp_json:
            self.session_id = resp_json['sessionId']
        if "noteId" in resp_json:
            self.note_id = resp_json['noteId']
        if "interpreter" in resp_json:
            self.interpreter = resp_json['interpreter']
        if "state" in resp_json:
            self.state = resp_json['state']
        if "weburl" in resp_json:
            self.weburl = resp_json['weburl']
        if "startTime" in resp_json:
            self.start_time = resp_json['startTime']


class ZeppelinClient:

    def __init__(self, client_config):
        self.client_config = client_config
        self.zeppelin_rest_url = client_config.get_zeppelin_rest_url()
        self.session = requests.Session()

    def _check_response(self, resp):
        if resp.status_code != 200:
            raise Exception("Invoke rest api failed, status code: {}, status text: {}".format(
                resp.status_code, resp.text))

    def get_version(self):
        resp = self.session.get(self.zeppelin_rest_url + "/api/version")
        self._check_response(resp)
        return resp.json()['body']['version']

    def login(self, user_name, password, knox_sso = None):
        if knox_sso:
            self.session.auth = (user_name, password)
            resp = self.session.get(knox_sso + "?originalUrl=" + self.zeppelin_rest_url, verify=False)
            if resp.status_code != 200:
                raise Exception("Knox SSO login fails, status: {}, status_text: {}" \
                    .format(resp.status_code, resp.text))
            resp = self.session.get(self.zeppelin_rest_url + "/api/security/ticket")
            if resp.status_code != 200:
                raise Exception("Fail to get ticket after Knox SSO, status: {}, status_text: {}" \
                                .format(resp.status_code, resp.text))
        else:
            resp = self.session.post(self.zeppelin_rest_url + "/api/login",
                                     data = {'userName': user_name, 'password': password})
            self._check_response(resp)

    def create_note(self, note_path, default_interpreter_group = 'spark'):
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook",
                                 json =  {'name' : note_path, 'defaultInterpreterGroup': default_interpreter_group})
        self._check_response(resp)
        return resp.json()['body']

    def delete_note(self, note_id):
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/" + note_id)
        self._check_response(resp)

    def query_note_result(self, note_id):
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook/" + note_id)
        self._check_response(resp)
        note_json = resp.json()['body']
        return NoteResult(note_json)

    def execute_note(self, note_id, params = {}):
        self.submit_note(note_id, params)
        return self.wait_until_note_finished(note_id)

    def submit_note(self, note_id, params = {}):
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/job/" + note_id,
                          params = {'blocking': 'false', 'isolated': 'true'},
                          json = {'params': params})
        self._check_response(resp)
        return self.query_note_result(note_id)

    def wait_until_note_finished(self, note_id):
        while True:
            note_result = self.query_note_result(note_id)
            logging.info("note_is_running: " + str(note_result.is_running))
            if not note_result.is_running:
                return note_result
            time.sleep(self.client_config.get_query_interval())

    def add_paragraph(self, note_id, title, text):
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph", json = {'title': title, 'text': text})
        self._check_response(resp)
        return resp.json()['body']

    def update_paragraph(self, note_id, paragraph_id, title, text):
        resp = self.session.put(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph/" + paragraph_id,
                                json = {'title' : title, 'text' : text})
        self._check_response(resp)

    def execute_paragraph(self, note_id, paragraph_id, params = {}, session_id = ""):
        self.submit_paragraph(note_id, paragraph_id, params, session_id)
        return self.wait_until_paragraph_finished(note_id, paragraph_id)

    def submit_paragraph(self, note_id, paragraph_id, params = {}, session_id = ""):
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/job/" + note_id + "/" + paragraph_id,
                                 params = {'sessionId': session_id},
                                 json = {'params': params})
        self._check_response(resp)
        return self.query_paragraph_result(note_id, paragraph_id)

    def query_paragraph_result(self, note_id, paragraph_id):
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph/" + paragraph_id)
        self._check_response(resp)
        return ParagraphResult(resp.json()['body'])

    def wait_until_paragraph_finished(self, note_id, paragraph_id):
        while True:
            paragraph_result = self.query_paragraph_result(note_id, paragraph_id)
            logging.debug("paragraph_status:" + paragraph_result.status)
            if paragraph_result.is_completed():
                return paragraph_result
            time.sleep(self.client_config.get_query_interval())

    def cancel_paragraph(self, note_id, paragraph_id):
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/job/" + note_id + "/" + paragraph_id)
        self._check_response(resp)

    def new_session(self, interpreter):
        resp = self.session.post(self.zeppelin_rest_url + "/api/session",
                          params = {'interpreter': interpreter})
        self._check_response(resp)
        return SessionInfo(resp.json()['body'])

    def stop_session(self, session_id):
        resp = self.session.delete(self.zeppelin_rest_url + "/api/session/" + session_id)
        self._check_response(resp)

    def get_session(self, session_id):
        resp = self.session.get(self.zeppelin_rest_url + "/api/session/" + session_id)
        if resp.status_code == 404:
            raise Exception("No such session: " + session_id)

        self._check_response(resp)
        return SessionInfo(resp.json()['body'])

    def next_session_paragraph(self, note_id, max_statement):
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/" + note_id +"/paragraph/next",
                                 params= {'maxParagraph' : max_statement})
        self._check_response(resp)
        return resp.json()['message']


if __name__ == "__main__":

    client_config = ClientConfig("https://knox.c-9181957fabf52f7e.cn-hangzhou.databricks.aliyuncs.com:8443/gateway/cluster-topo/zeppelin/")
    client = ZeppelinClient(client_config)
    client.login("zongze_ram", "1234qwer", knox_sso="https://knox.c-9181957fabf52f7e.cn-hangzhou.databricks.aliyuncs.com:8443/gateway/knoxsso/api/v1/websso")
    print('version:' + client.get_version())

    note_id = None;
    try:
        note_id = client.create_note('/test/note_18', 'spark')
        note_result = client.query_note_result(note_id)
        print(note_result)
        client.submit_note(note_id)
        note_result = client.wait_until_note_finished(note_id)
        print("note is finished")
        print("note_result: " + str(note_result))

        paragraph_id = client.add_paragraph(note_id, 'title', '%sh pwd')
        client.submit_paragraph(note_id, paragraph_id)
        client.wait_until_paragraph_finished(note_id, paragraph_id)
        note_result = client.query_note_result(note_id)
        print("note is finished")
        print("note_result: " + str(note_result))
        print(note_result)
    finally:
        if note_id:
            pass
            # client.delete_note(note_id)

