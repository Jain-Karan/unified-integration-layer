import json
from pathlib import Path

import pytest

from uil import synthetic
from uil.lake import Lake


@pytest.fixture(scope="session")
def feeds(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("feeds")
    synthetic.build(root, seed=11, scale=1)
    return root


@pytest.fixture(scope="session")
def truth(feeds) -> dict:
    return json.loads((feeds / "truth.json").read_text())


@pytest.fixture
def lake(tmp_path) -> Lake:
    return Lake(tmp_path / "lake")
