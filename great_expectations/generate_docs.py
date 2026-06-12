# generate_docs.py
from ge_setup import get_context

def build_data_docs():
    context = get_context()
    context.build_data_docs()