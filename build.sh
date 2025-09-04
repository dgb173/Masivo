#!/usr/bin/env bash
# exit on error
set -o errexit

# 1. Instala las dependencias de Python
pip install -r requirements.txt

# 2. Instala los navegadores de Playwright
#    Evita dependencias del sistema para agilizar el despliegue.
playwright install chromium
