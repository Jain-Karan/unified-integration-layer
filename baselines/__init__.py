"""Hand-written, per-source pipelines: what each integration looks like without the framework.

They exist to make the code-size comparison in `benchmarks/loc.py` honest. Each one is the
straightforward script a team would write for that one source: its own file handling, its own
parsing of the source's quirks, its own error routing. They share only the canonical models and
the lake (both of which exist either way). `tests/test_baselines.py` proves each produces
exactly the same canonical rows and the same rejects as the declarative mapping.
"""
