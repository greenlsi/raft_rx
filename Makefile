PYTHONPATH_PY = PYTHONPATH=python/src:../rxnet/python

all: test

test:
	$(PYTHONPATH_PY) python3 -m pytest -q python/tests
	$(MAKE) -C c test

examples:
	$(PYTHONPATH_PY) python3 python/examples/kv_cluster.py
	$(MAKE) -C c build/raft_kv_cluster
	./c/build/raft_kv_cluster

.PHONY: all test examples
