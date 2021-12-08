import json


class NoteResult:

    def __init__(self, note_json):
        self.note_json = note_json
        self.note_id = note_json['id']
        self.is_running = False
        if 'info' in note_json:
            info_json = note_json['info']
            if 'isRunning' in info_json:
                self.is_running = bool(info_json['isRunning'])
        self.paragraphs = []
        if 'paragraphs' in note_json:
            paragraph_json_array = note_json['paragraphs']
            self.paragraphs = list(map(lambda x : ParagraphResult(x), paragraph_json_array))

    def is_success(self):
        for p in self.paragraphs:
            if p.status != 'FINISHED':
                return False
        return True

    def get_errors(self):
        for p in self.paragraphs:
            if p.status != 'FINISHED':
                return "Paragraph {0} is {1}.\n\nText: {2}\n\nResults:{3}\n\nAssociated job urls: {4}\n\nJson:{5}"\
                    .format(p.paragraph_id, p.status, p.text, '\n'.join(list(map(lambda x: x[1], p.results))), str(p.jobUrls), p.paragraph_json)
        return "All paragraphs are finished successfully!"

    def __repr__(self):
        return json.dumps(self.note_json, indent=2)


class ParagraphResult:

    def __init__(self, paragraph_json):
        self.paragraph_json = paragraph_json
        self.paragraph_id = paragraph_json['id']
        self.text = paragraph_json['text']
        self.status = paragraph_json['status']
        self.progress = 0
        if 'progress' in paragraph_json:
            self.progress = int(paragraph_json['progress'])
        if 'results' in paragraph_json:
            results_json = paragraph_json['results']
            msg_array = results_json['msg']
            self.results = list(map(lambda x : (x['type'], x['data']), msg_array))
        else:
            self.results = []

        self.jobUrls = []
        if 'runtimeInfos' in paragraph_json:
            runtimeInfos_json = paragraph_json['runtimeInfos']
            if 'jobUrl' in runtimeInfos_json:
                jobUrl_json = runtimeInfos_json['jobUrl']
                if 'values' in jobUrl_json:
                    jobUrl_values = jobUrl_json['values']
                    self.jobUrls = list(map(lambda x: x['jobUrl'], filter(lambda x : 'jobUrl' in x, jobUrl_values)))


    def is_completed(self):
        return self.status in ['FINISHED', 'ERROR', 'ABORTED']

    def is_running(self):
        return self.status == 'RUNNING'

    def is_success(self):
        return self.status == 'FINISHED'

    def get_errors(self):
        if self.status != 'FINISHED':
            return "Paragraph {0} is failed.\n\nText: {1}\n\nResults:{2}\n\nAssociated job urls: {3}\n\nJson:{4}"\
                .format(self.paragraph_id, self.text, '\n'.join(list(map(lambda x: x[1], self.results))), str(self.jobUrls), self.paragraph_json)
        return "Paragraph is finished successfully!"

    def __repr__(self):
        return json.dumps(self.paragraph_json, indent=2)


class ExecuteResult:

    def __init__(self, paragraph_result):
        self.statement_id = paragraph_result.paragraph_id
        self.status = paragraph_result.status
        self.progress = paragraph_result.progress
        self.results = paragraph_result.results
        self.jobUrls = paragraph_result.jobUrls

    def __repr__(self):
        return str(self.__dict__)

    def is_success(self):
        return self.status == 'FINISHED'

