SHELL:=/bin/bash

clean: clean-build clean-pyc

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	rm -fr .pytest_cache/
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

install: clean ## install the package to the active Python's site-packages
	python setup.py install

test: test-py2 test-py3
test-py2: test-setpy2 test-run
test-py3: test-setpy3 test-run
test-setpy3:
	pyenv global 3.6.5
test-setpy2:
	pyenv global 2.7.6
test-run: install
	python -m pytest

install-pytest:
	sudo apt-get install -y python-logilab-common
	pip install -U pip pytest pytest-cov pytest-socket

install-pyenv:
	sudo apt-get install -y git
	rm -rf ~/.pyenv
	rm -f ~/bin/pyenv
	git clone https://github.com/pyenv/pyenv.git ~/.pyenv
	ln -s ~/.pyenv/bin/pyenv ~/bin/pyenv
	pyenv init -
	pyenv install 2.7.6
	pyenv install 3.6.5

dependencies:
	sudo apt-get install -y \
		python-setuptools \
		build-essential \
		cython \
		python-dev \
		libxslt1-dev \
		libxml2-dev \
		zlib1g-dev \
		libevent-dev \
		libffi-dev \
		libfreetype6-dev \
		liblcms2-dev \
		cmake \
		imagemagick \
		libssl-dev \
		libzmq-dev \
		libmysqlclient-dev \
		python-pip
	pip install -U pip

install-dev-env: dependencies install-pytest install install-pyenv