import unittest

from pyzeppelin.config import ClientConfig
from pyzeppelin.result import NoteResult
import json

from pyzeppelin.zeppelin_client import ZeppelinClient


class TestZeppelinClient(unittest.TestCase):

    def test_note_operation(self):
        client_config = ClientConfig("http://localhost:8080")
        client = ZeppelinClient(client_config)
        #client.login('user1', 'password2')
        client.get_version()
        note_id = client.create_note('/pyzeppelin/test/note_1')
        client.delete_note(note_id)

        with self.assertRaises(Exception) as context:
            client.delete_note('invalid_note_id')
        self.assertTrue('No such note' in str(context.exception))

        # note2_id = None
        # try:
        #     note2_id = client.create_note('/pyzeppelin/test/note_2')
        #     with self.assertRaises(Exception) as context:
        #         client.create_note('/pyzeppelin/test/note_2')
        #     self.assertTrue('existed' in str(context.exception))
        # finally:
        #     if note2_id:
        #         client.delete_note(note2_id)


        note_id = client.create_note('/pyzeppelin/test/note_1')
        client.add_paragraph(note_id, 'shell example', "%sh echo 'hello world'")
        cloned_note_id = client.clone_note(note_id, '/pyzeppelin/test/cloned_note_1')
        cloned_note = client.query_note_result(cloned_note_id)
        self.assertEqual(1, len(cloned_note.paragraphs))

    def test_execute_paragraph(self):
        client_config = ClientConfig("http://localhost:8080")
        client = ZeppelinClient(client_config)
        client.login('user1', 'password2')

        note_id = None
        try:
            note_id = client.create_note('/pyzeppelin/test/note_1')
            paragraph_id = client.add_paragraph(note_id, 'shell example', "%sh echo 'hello world'")
            paragraph_result = client.execute_paragraph(note_id, paragraph_id)
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello world\n', paragraph_result.results[0][1])

            # # dynamic forms
            paragraph_id = client.add_paragraph(note_id, "dynamic form example", "%sh echo 'hello ${name=abc}'")
            paragraph_result = client.execute_paragraph(note_id, paragraph_id)
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello abc\n', paragraph_result.results[0][1])

            # run paragraph with parameters
            paragraph_result = client.execute_paragraph(note_id, paragraph_id, params = {'name': 'zeppelin'})
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello zeppelin\n', paragraph_result.results[0][1])

        finally:
            if note_id:
                client.delete_note(note_id)


    def test_submit_paragraph(self):
        client_config = ClientConfig("http://localhost:8080")
        client = ZeppelinClient(client_config)
        client.login('user1', 'password2')

        note_id = None
        try:
            note_id = client.create_note('/pyzeppelin/test/note_1')
            paragraph_id = client.add_paragraph(note_id, 'shell example', "%sh echo 'hello world'")
            paragraph_result = client.submit_paragraph(note_id, paragraph_id)
            paragraph_result = client.wait_until_paragraph_finished(note_id, paragraph_id)
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello world\n', paragraph_result.results[0][1])

            # # dynamic forms
            paragraph_id = client.add_paragraph(note_id, "dynamic form example", "%sh echo 'hello ${name=abc}'")
            paragraph_result = client.submit_paragraph(note_id, paragraph_id)
            paragraph_result = client.wait_until_paragraph_finished(note_id, paragraph_id)
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello abc\n', paragraph_result.results[0][1])

            # run paragraph with parameters
            paragraph_result = client.submit_paragraph(note_id, paragraph_id, params = {'name': 'zeppelin'})
            paragraph_result = client.wait_until_paragraph_finished(note_id, paragraph_id)
            self.assertEqual('FINISHED', paragraph_result.status)
            self.assertEqual(1, len(paragraph_result.results))
            self.assertEqual('TEXT', paragraph_result.results[0][0])
            self.assertEqual('hello zeppelin\n', paragraph_result.results[0][1])

        finally:
            if note_id:
                client.delete_note(note_id)


    def test_execute_note(self):
        client_config = ClientConfig("http://localhost:8080")
        client = ZeppelinClient(client_config)
        client.login('user1', 'password2')

        with self.assertRaises(Exception) as context:
            client.execute_note('invalid_note_id')
        self.assertTrue('No such note' in str(context.exception))

        note_id = None
        try:
            note_id = client.create_note('/pyzeppelin/test/note_1')
            paragraph_id = client.add_paragraph(note_id, 'shell example', "%sh echo 'hello world'")
            note_result = client.execute_note(note_id)
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello world\n', note_result.paragraphs[0].results[0][1])

            # # dynamic forms
            paragraph_id = client.update_paragraph(note_id, paragraph_id, "dynamic form example", "%sh echo 'hello ${name=abc}'")
            note_result = client.execute_note(note_id)
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello abc\n', note_result.paragraphs[0].results[0][1])

            # run paragraph with parameters
            note_result = client.execute_note(note_id, params = {'name' : 'zeppelin'})
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello zeppelin\n', note_result.paragraphs[0].results[0][1])

        finally:
            if note_id:
                client.delete_note(note_id)


    def test_submit_note(self):
        client_config = ClientConfig("http://localhost:8080")
        client = ZeppelinClient(client_config)
        client.login('user1', 'password2')

        with self.assertRaises(Exception) as context:
            client.submit_note('invalid_note_id')
        self.assertTrue('No such note' in str(context.exception))

        note_id = None
        try:
            note_id = client.create_note('/pyzeppelin/test/note_1')
            paragraph_id = client.add_paragraph(note_id, 'shell example', "%sh echo 'hello world'")
            note_result = client.submit_note(note_id)
            note_result = client.wait_until_note_finished(note_id)
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello world\n', note_result.paragraphs[0].results[0][1])

            # # dynamic forms
            paragraph_id = client.update_paragraph(note_id, paragraph_id, "dynamic form example", "%sh echo 'hello ${name=abc}'")
            note_result = client.submit_note(note_id)
            note_result = client.wait_until_note_finished(note_id)
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello abc\n', note_result.paragraphs[0].results[0][1])

            # run paragraph with parameters
            note_result = client.submit_note(note_id, params = {'name' : 'zeppelin'})
            note_result = client.wait_until_note_finished(note_id)
            self.assertEqual(False, note_result.is_running)
            self.assertEqual(1, len(note_result.paragraphs))
            self.assertEqual('TEXT', note_result.paragraphs[0].results[0][0])
            self.assertEqual('hello zeppelin\n', note_result.paragraphs[0].results[0][1])

        finally:
            if note_id:
                client.delete_note(note_id)


if __name__ == '__main__':
    unittest.main()
