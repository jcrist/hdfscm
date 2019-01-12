#!/usr/bin/env bash
set -xe

cd hdfscm
py.test hdfscm --verbose
flake8 hdfscm
