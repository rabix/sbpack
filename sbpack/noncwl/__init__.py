import os

PACKAGE_PATH = os.path.abspath(os.path.dirname(__file__))
JS_PATH = os.path.join(PACKAGE_PATH, 'js_templates')


def read_js_template(file_name):
    with open(os.path.join(JS_PATH, file_name), 'r') as f:
        return f.read()
