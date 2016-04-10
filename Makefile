
travis: pytest coverage

tox:
	tox

pytest:
	py.test

coverage:
	py.test --cov=.

all: tox pytest coverage

test: pytest

.PHONY : pytest tox coverage travis
