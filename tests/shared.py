import pathlib
import sys


def add_mypandas_to_path():
    MYPANDAS_PATH = str(pathlib.Path(__file__).absolute().parent.parent / "src")
    sys.path.insert(0, MYPANDAS_PATH)
