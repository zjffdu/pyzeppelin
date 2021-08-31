#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
from pyzeppelin.config import ClientConfig
from pyzeppelin.notebook import Note
from pyzeppelin.notebook import Paragraph
import time
import logging


class SessionInfo:

    def __init__(self, resp_json):
        """

        :param resp_json:
        """
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
    """
    Low leve of Zeppelin SDK, this is used to interact with Zeppelin in note/paragraph abstraction layer.
    """
    def __init__(self, client_config):
        self.client_config = client_config
        self.zeppelin_rest_url = client_config.get_zeppelin_rest_url()
        self.session = requests.Session()

    def _check_response(self, resp):
        if resp.status_code != 200:
            raise Exception("Invoke rest api failed, status code: {}, status text: {}".format(
                resp.status_code, resp.text))

    def get_version(self):
        """
        Return Zeppelin version
        :return:
        """
        resp = self.session.get(self.zeppelin_rest_url + "/api/version")
        self._check_response(resp)
        return resp.json()['body']['version']

    def login(self, user_name, password, knox_sso = None):
        """
        Login to Zeppelin, use knox_sso if it is provided.
        :param user_name:
        :param password:
        :param knox_sso:
        :return:
        """
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
        """
        Create a new note with give note_path and default_interpreter_group
        :param note_path:
        :param default_interpreter_group:
        :return:
        """
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook",
                                 json =  {'name' : note_path, 'defaultInterpreterGroup': default_interpreter_group})
        self._check_response(resp)
        return resp.json()['body']

    def delete_note(self, note_id):
        """
        Delete a note with give note_id
        :param note_id:
        :return:
        """
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/" + note_id)
        self._check_response(resp)

    def query_note_result(self, note_id):
        """
        Query note result via Zeppelin rest api and convert the returned json to NoteResult
        :param note_id:
        :return:
        """
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook/" + note_id)
        self._check_response(resp)
        note_json = resp.json()['body']
        return Note(note_json)

    def execute_note(self, note_id, params = {}):
        """
        Execute give note with params, block until note execution is finished.
        :param note_id:
        :param params:
        :return:
        """
        self.submit_note(note_id, params)
        return self.wait_until_note_finished(note_id)

    def submit_note(self, note_id, params = {}):
        """
        Execute give note with params, return once submission is finished. It is non-blocking api,
        won't wait for the completion of note execution.
        :param note_id:
        :param params:
        :return:
        """
        logging.info("Submitting note: " + note_id + " with params: " + str(params))
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/job/" + note_id,
                          params = {'blocking': 'false', 'isolated': 'true', 'reload': 'true'},
                          json = {'params': params})
        self._check_response(resp)
        return self.query_note_result(note_id)

    def wait_until_note_finished(self, note_id):
        """
        Wait until note execution is finished.
        :param note_id:
        :return:
        """
        while True:
            note_result = self.query_note_result(note_id)
            logging.info("note_is_running: " + str(note_result.is_running))
            if not note_result.is_running:
                return note_result
            time.sleep(self.client_config.get_query_interval())

    def reload_note_list(self):
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook", params = {'reload': 'true'})
        self._check_response(resp)
        return resp.json()['body']

    def get_note(self, note_id, reload = False):
        """
        Get specified note.
        :param note_id:
        :param reload:
        :return:
        """
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook/" + note_id, params = {'reload': reload})
        self._check_response(resp)
        return resp.json()['body']

    def clone_note(self, note_id, dest_note_path):
        """
        Clone specific note to another location.
        :param note_id:
        :param dest_note_path:
        :return:
        """
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/" + note_id, json = {'name': dest_note_path})
        self._check_response(resp)
        return resp.json()['body']

    def add_paragraph(self, note_id, title, text):
        """
        Add paragraph to specific note at the last paragraph
        :param note_id:
        :param title:
        :param text:
        :return:
        """
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph", json = {'title': title, 'text': text})
        self._check_response(resp)
        return resp.json()['body']

    def update_paragraph(self, note_id, paragraph_id, title, text):
        """
        update specified paragraph with given title and text
        :param note_id:
        :param paragraph_id:
        :param title:
        :param text:
        :return:
        """
        resp = self.session.put(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph/" + paragraph_id,
                                json = {'title' : title, 'text' : text})
        self._check_response(resp)

    def execute_paragraph(self, note_id, paragraph_id, params = {}, session_id = "", isolated = False):
        """
        Blocking api, execute specified paragraph with given params
        :param note_id:
        :param paragraph_id:
        :param params:
        :param session_id:
        :param isolated:
        :return:
        """
        self.submit_paragraph(note_id, paragraph_id, params, session_id, isolated)
        return self.wait_until_paragraph_finished(note_id, paragraph_id)

    def submit_paragraph(self, note_id, paragraph_id, params = {}, session_id = "", isolated = False):
        """
        Non-blocking api, execute specified paragraph with given params.
        :param note_id:
        :param paragraph_id:
        :param params:
        :param session_id:
        :param isolated:
        :return:
        """
        logging.info("Submitting paragraph: " + paragraph_id + " with params: " + str(params))
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/job/" + note_id + "/" + paragraph_id,
                                 params = {'sessionId': session_id, 'isolated': isolated, 'reload': 'true'},
                                 json = {'params': params})
        self._check_response(resp)
        return self.query_paragraph_result(note_id, paragraph_id)

    def query_paragraph_result(self, note_id, paragraph_id):
        """
        Query specified paragraph result.
        :param note_id:
        :param paragraph_id:
        :return:
        """
        resp = self.session.get(self.zeppelin_rest_url + "/api/notebook/" + note_id + "/paragraph/" + paragraph_id)
        self._check_response(resp)
        return Paragraph(resp.json()['body'])

    def wait_until_paragraph_finished(self, note_id, paragraph_id):
        """
        Wait until specified paragraph execution is finished
        :param note_id:
        :param paragraph_id:
        :return:
        """
        while True:
            paragraph_result = self.query_paragraph_result(note_id, paragraph_id)
            logging.debug("paragraph_status:" + paragraph_result.status)
            if paragraph_result.is_completed():
                return paragraph_result
            time.sleep(self.client_config.get_query_interval())

    def cancel_paragraph(self, note_id, paragraph_id):
        """
        Cancel specified paragraph execution.
        :param note_id:
        :param paragraph_id:
        :return:
        """
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/job/" + note_id + "/" + paragraph_id)
        self._check_response(resp)

    def cancel_note(self, note_id):
        """
        Cancel specified note execution.
        :param note_id:
        :return:
        """
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/job/" + note_id)
        self._check_response(resp)
        resp = self.session.delete(self.zeppelin_rest_url + "/api/notebook/job/" + note_id)
        self._check_response(resp)

    def new_session(self, interpreter):
        """
        Create new ZSession for specified interpreter
        :param interpreter:
        :return:
        """
        resp = self.session.post(self.zeppelin_rest_url + "/api/session",
                          params = {'interpreter': interpreter})
        self._check_response(resp)
        return SessionInfo(resp.json()['body'])

    def stop_session(self, session_id):
        """
        Stop specified ZSession
        :param session_id:
        :return:
        """
        resp = self.session.delete(self.zeppelin_rest_url + "/api/session/" + session_id)
        self._check_response(resp)

    def get_session(self, session_id):
        """
        Get SessionInfo of specified session_id
        :param session_id:
        :return:
        """
        resp = self.session.get(self.zeppelin_rest_url + "/api/session/" + session_id)
        if resp.status_code == 404:
            raise Exception("No such session: " + session_id)

        self._check_response(resp)
        return SessionInfo(resp.json()['body'])

    def next_session_paragraph(self, note_id, max_statement):
        """
        Create a new paragraph for specified session.
        :param note_id:
        :param max_statement:
        :return:
        """
        resp = self.session.post(self.zeppelin_rest_url + "/api/notebook/" + note_id +"/paragraph/next",
                                 params= {'maxParagraph' : max_statement})
        self._check_response(resp)
        return resp.json()['message']


if __name__ == "__main__":

    client_config = ClientConfig("")
    client = ZeppelinClient(client_config)
    client.login("", "", knox_sso="https://:8443/gateway/knoxsso/api/v1/websso")
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

