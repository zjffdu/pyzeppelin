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

from pyzeppelin.config import ClientConfig
from pyzeppelin.zeppelin_client import ZeppelinClient
from pyzeppelin.notebook import ExecuteResult

import logging


class ZSession:
    """
    High abstraction layer of Zeppelin SDK, this is used to interact with Zeppelin in session layer.
    There's no Zeppelin concept(note, paragraph, etc) in this layer.
    """
    def __init__(self, client_config, interpreter, intp_properties = {}, max_statement = 100):
        self.client_config = client_config
        self.zeppelin_client = ZeppelinClient(client_config)
        self.interpreter = interpreter
        self.intp_properties = intp_properties
        self.max_statement = max_statement

    def login(self, user_name, password):
        """
        Login Zeppelin with specified user_name & password
        :param user_name:
        :param password:
        :return:
        """
        self.zeppelin_client.login(user_name, password)

    def start(self):
        """
        Start this ZSession, underneath it starts a new isolated Zeppelin interpreter process.
        :return:
        """
        logging.info("starting session for interpreter: {}, properties: {}".format(self.interpreter, str(self.intp_properties)))
        self.session_info = self.zeppelin_client.new_session(self.interpreter)
        conf_code = "%" + self.interpreter + ".conf\n"
        conf_code += '\n'.join((k + " " + v) for (k, v) in self.intp_properties.items())
        conf_paragraph_id = self.zeppelin_client.add_paragraph(self.session_info.note_id, "Session Configuration", conf_code)
        conf_paragraph_result = self.zeppelin_client.execute_paragraph(self.session_info.note_id, conf_paragraph_id, session_id = self.session_info.session_id)
        if conf_paragraph_result.status != 'FINISHED':
            raise Exception("Fail to configure session: " + str(conf_paragraph_result))

        init_paragraph_id = self.zeppelin_client.add_paragraph(self.session_info.note_id, 'Session Init', "%" + self.interpreter + "(init=true)")
        init_paragraph_result = self.zeppelin_client.execute_paragraph(self.session_info.note_id, init_paragraph_id, session_id = self.session_info.session_id)
        if init_paragraph_result.status != 'FINISHED':
            raise Exception("Fail to init session: " + str(init_paragraph_result))

        logging.info("session started")
        self.session_info = self.zeppelin_client.get_session(self.session_info.session_id);

    def stop(self):
        """
        Stop this ZSession, underneath it stop the associated Zeppelin interpreter process.
        :return:
        """
        if self.session_info and self.session_info.session_id:
            self.zeppelin_client.stop_session(self.session_info.session_id)
            logging.info("session {} is stopped".format(self.session_info.session_id))

    def execute(self, code, sub_interpreter = None, local_properties = None):
        """
        Blocking api, execute a piece of code with specified sub_interpreter & local_properties,
        wait until the execution is finished.
        :param code:
        :param sub_interpreter:
        :param local_properties:
        :return:
        """
        script_text = "%" + self.interpreter
        if sub_interpreter:
            script_text += "." + sub_interpreter
        if local_properties:
            script_text = script_text + '(' + \
                          ','.join([(k + '=' + v) for (k, v) in local_properties.items()]) + \
                          ')'

        script_text = script_text + ' ' + code
        next_paragraph_id = self.zeppelin_client.next_session_paragraph(self.session_info.note_id, self.max_statement)
        self.zeppelin_client.update_paragraph(self.session_info.note_id, next_paragraph_id, "", script_text)
        paragraph_result = self.zeppelin_client.execute_paragraph(self.session_info.note_id, next_paragraph_id, session_id = self.session_info.session_id)
        return ExecuteResult(paragraph_result)

    def submit(self, code, sub_interpreter = None, local_properties = None):
        """
        Non-blocking api, submit a piece of code with specified sub_interpreter & local_properties.
        Won't wait for the execution completion.
        :param code:
        :param sub_interpreter:
        :param local_properties:
        :return:
        """
        script_text = "%" + self.interpreter
        if sub_interpreter:
            script_text += "." + sub_interpreter
        if local_properties:
            script_text = script_text + '(' + \
                          ','.join([(k + '=' + v) for (k, v) in local_properties.items()]) + \
                          ')'

        script_text = script_text + ' ' + code
        next_paragraph_id = self.zeppelin_client.next_session_paragraph(self.session_info.note_id, self.max_statement)
        self.zeppelin_client.update_paragraph(self.session_info.note_id, next_paragraph_id, "", script_text)
        paragraph_result = self.zeppelin_client.submit_paragraph(self.session_info.note_id, next_paragraph_id, session_id = self.session_info.session_id)
        return ExecuteResult(paragraph_result)

    def wait_util_finished(self, statement_id):
        """

        :param statement_id:
        :return:
        """
        paragraph_result = self.zeppelin_client.wait_until_paragraph_finished(self.session_info.note_id, statement_id)
        return ExecuteResult(paragraph_result)

    def cancel(self, statement_id):
        """
        Cancel specified statement_id, underneath it just cancel one paragraph.
        :param statement_id:
        :return:
        """
        self.zeppelin_client.cancel(self.session_info.note_id, statement_id)

    def query_statement(self, statement_id):
        """
        Query specified statement result, underneath it query the paragraph result.
        :param statement_id:
        :return:
        """
        paragraph_result = self.zeppelin_client.query_paragraph_result(self.session_info.note_id, statement_id)
        return ExecuteResult(paragraph_result)

    def session_id(self):
        """

        :return:
        """
        if self.session_info:
            return self.session_info.session_id
        else:
            return None



if __name__ == "__main__":

    client_config = ClientConfig("http://localhost:8080")
    session = ZSession(client_config, "python")
    session.login("user1", "password2")

    try:
        session.start()
        print("session_id:" + session.session_id())
        result = session.execute("1/0")
        print("execute_result: " + str(result))
    finally:
        session.stop()

