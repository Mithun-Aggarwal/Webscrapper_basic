#!/usr/bin/env bash
set -e
python -m crawler discover --config config.example.yml
python -m crawler download --config config.example.yml
