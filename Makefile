SHELL := sh
.SHELLFLAGS := -c
MAKEFLAGS += --warn-undefined-variables
.RECIPEPREFIX = >
.DEFAULT_GOAL := help
.PHONY: help clean deps test install build check publish publishprod

ifeq ($(OS),Windows_NT)
  RM = rmdir /s /q
  PYTHON = python
else
  RM = rm -rf
  PYTHON = python3
endif

PACKAGE_NAME = ezpq


help:
> echo halp

clean:
> -$(RM) build dist .ipynb_checkpoints

deps:
> $(PYTHON) -m pip install -r requirements.txt

test: deps
> $(PYTHON) -m unittest discover --failfast -t . -s ./tests -p test_*.py

install: deps
> pip install .

build: clean deps
> pip install setuptools wheel build
> $(PYTHON) -m build

build-docs: install
> $(PYTHON) -m pip install pdoc
> pdoc -o public --docformat google --search --show-source $(PACKAGE_NAME)

build-readme: deps
> $(PYTHON) -m pip install jupyter ipywidgets ipykernel nbconvert docutils
> jupyter nbconvert --execute *.ipynb --to markdown

check: build
> $(PYTHON) -m pip install twine
> twine check --strict dist/*

publish: check
> twine upload --non-interactive --skip-existing -r testpypi dist/*

publish-prod: check
> twine upload --non-interactive --skip-existing -r pypi dist/*
