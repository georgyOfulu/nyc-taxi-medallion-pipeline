import pytest
import sys

sys.dont_write_bytecode = True
pytest.main(['test_functions.py', '-p', 'no:cacheprovider'])