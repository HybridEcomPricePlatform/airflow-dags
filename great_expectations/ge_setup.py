# ge_setup.py
import great_expectations as gx

def get_context():
    """
    Contexte GE persistant, à monter dans le conteneur Airflow.
    Le dossier doit être versionné dans le repo (suites, checkpoints, data_docs).
    """
    context = gx.get_context(
        mode="file",
        project_root_dir="/opt/airflow/great_expectations"
    )
    return context