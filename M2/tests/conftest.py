import pytest

from kestrel_m1 import reference as rf


@pytest.fixture(scope="session")
def ref():
    return rf.load_reference()
