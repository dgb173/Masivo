#!/usr/bin/env bash
# exit on error
set -o errexit

# 1. Instala las dependencias de Python
pip install -r requirements.txt

# 2. Instala los navegadores de Playwright
#    Este comando es crucial para que el scraper funcione en la nube.
playwright install --with-deps
