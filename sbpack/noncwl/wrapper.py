import logging
from sbpack.noncwl.constants import REMOVE_INPUT_KEY


class Wrapper:
    inputs = list()
    outputs = list()
    app_content = dict()
    class_ = None
    cwl_version = None
    arguments = None
    requirements = None
    hints = None
    doc = None
    revision_note = None

    def __init__(self):
        pass

    def get_input(self, id_):
        for inp in self.inputs:
            if inp.get('id', '') == id_:
                return inp
        else:
            logging.warning(f'Input with id <{id_}> not found.')

    def add_input(self, inp):
        for input_ in self.inputs:
            id_ = inp.get('id')
            if input_.get('id') == id_:
                # raise an exception or warning
                logging.warning(f'Input with id <{id_}> already exists. '
                                f'Skipping...')

        self.inputs.append(inp)

    def safe_add_input(self, inp):
        all_input_ids = [i['id'] for i in self.inputs if 'id' in i]
        input_id = inp.get('id')
        temp_id = input_id
        i = 0
        while temp_id in all_input_ids:
            i += 1
            temp_id = f"{input_id}_{i}"

        inp['id'] = temp_id
        self.add_input(inp)

        return inp

    def update_input(self, inp):
        id_ = inp.get('id')
        for input_ in self.inputs:
            if input_['id'] == id_:
                input_.update(inp)
                for key in input_.copy():
                    if input_[key] == REMOVE_INPUT_KEY:
                        input_.pop(key)
                break
        else:
            raise KeyError(
                f'Input with id <{id_}> not found.'
            )

    def get_output(self, id_):
        for out in self.outputs:
            if out.get('id', '') == id_:
                return out
        else:
            logging.warning(f'Output with id <{id_}> not found.')

    def add_output(self, out):
        for output in self.outputs:
            id_ = out.get('id')
            if output.get('id') == id_:
                # raise an exception or warning
                logging.warning(f'Output with id <{id_}> already exists. '
                                f'Skipping...')
        self.outputs.append(out)

    def safe_add_output(self, out):
        all_output_ids = [o['id'] for o in self.outputs if 'id' in o]
        output_id = out.get('id')
        temp_id = output_id
        i = 0
        while temp_id in all_output_ids:
            i += 1
            temp_id = f"{output_id}_{i}"

        out['id'] = temp_id
        self.add_output(out)

        return out

    def update_output(self, out):
        id_ = out.get('id')
        for output in self.outputs:
            if output['id'] == id_:
                output.update(out)
                for key in output:
                    if output[key] == REMOVE_INPUT_KEY:
                        output.pop(key)
                break
        else:
            raise KeyError(
                f'Output with id <{id_}> not found.'
            )

    def add_requirement(self, requirement):
        if not self.requirements:
            self.requirements = list()

        for req in self.requirements:
            if req['class'] == requirement['class']:
                # check listings -> add missing -> break
                if requirement['class'] == 'InitialWorkDirRequirement' and \
                        'listing' in requirement:
                    if 'listing' not in req:
                        req['listing'] = []
                    req['listing'].extend(requirement['listing'])
                break
        else:
            # add new class
            self.requirements.append(requirement)

    def set_app_content(
            self, code_package=None, entrypoint=None, executor_version=None,
            **kwargs
    ):
        payload = dict()

        if code_package:
            payload['code_package'] = code_package
        if entrypoint:
            payload['entrypoint'] = entrypoint
        if executor_version:
            payload['executor_version'] = executor_version

        self.app_content.update(payload)

    def add_argument(self, arg):
        if not self.arguments:
            self.arguments = list()
        self.arguments.append(arg)

    def add_hint(self, hint):
        if not self.hints:
            self.hints = list()
        self.hints.append(hint)

    def add_docs(self, doc):
        self.doc = doc

    def add_revision_note(self, note):
        self.revision_note = note

    def load(self, schema):
        s_inputs = schema.get('inputs', [])
        for input_ in s_inputs:
            self.add_input(input_)

        s_outputs = schema.get('outputs', [])
        for output in s_outputs:
            self.add_output(output)

        s_app_content = schema.get('app_content', dict())
        self.set_app_content(**s_app_content)

        self.class_ = schema.get('class', None)
        self.cwl_version = schema.get('cwlVersion', None)

        s_arguments = schema.get('arguments', [])
        for argument in s_arguments:
            self.add_argument(argument)

        s_requirements = schema.get('requirements', [])
        for requirement in s_requirements:
            self.add_requirement(requirement)

        s_hints = schema.get('hints', [])
        for hint in s_hints:
            self.add_hint(hint)

        s_doc = schema.get('doc', None)
        if s_doc:
            self.add_docs(s_doc)

        s_revision_note = schema.get('sbg:revisionNote', None)
        if s_revision_note:
            self.add_revision_note(s_revision_note)

    def dump(self):
        wrapper = dict()

        if self.app_content:
            wrapper['app_content'] = self.app_content

        if self.doc:
            wrapper['doc'] = self.doc

        wrapper['inputs'] = self.inputs
        wrapper['outputs'] = self.outputs

        if self.arguments:
            wrapper['arguments'] = self.arguments

        if self.class_:
            wrapper['class'] = self.class_

        if self.cwl_version:
            wrapper['cwlVersion'] = self.cwl_version

        if self.requirements:
            wrapper['requirements'] = self.requirements

        if self.hints:
            wrapper['hints'] = self.hints

        if self.revision_note:
            wrapper['sbg:revisionNotes'] = self.revision_note

        return wrapper
