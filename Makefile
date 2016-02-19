all:
	@echo "Usage: make test or make test-force"

test:
	cd test/ && /usr/bin/python test-run.py

test-force:
	cd test/ && /usr/bin/python test-run.py --force

.PHONY: all test test-force
