from types import SimpleNamespace

import pytest

from kestrel_m1 import activity, audit, flows, siem
from kestrel_m1 import reference as rf


@pytest.fixture(scope="session")
def ref():
    return rf.load_reference()


@pytest.fixture(scope="session")
def pack(ref):
    """The full pack through every stage once, shared by all checkpoint tests."""
    act = activity.clean_activity(ref=ref)
    sess = activity.session_table(act.accepted)
    return SimpleNamespace(act=act, sess=sess, fl=flows.clean_flows(sess),
                           au=audit.clean_audit(sess, act.accepted), si=siem.clean_siem())
