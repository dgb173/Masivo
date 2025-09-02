# app.py - Servidor web principal (Flask)
from flask import Flask, render_template, abort, request
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import datetime
from pprint import pprint

# Importa las funciones del nuevo módulo de scraping
from modules.estudio_scraper import obtener_datos_completos_partido, format_ah_as_decimal_string_of

app = Flask(__name__)

# --- Lógica para la página principal (Scraper de partidos) ---
URL_NOWGOAL = "https://live20.nowgoal25.com/"

def parse_main_page_matches(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
    upcoming_matches = []
    now_utc = datetime.datetime.utcnow()

    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id: continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'): continue
        
        try:
            match_time = datetime.datetime.strptime(time_cell['data-t'], '%Y-%m-%d %H:%M:%S')
        except (ValueError, IndexError):
            continue

        if match_time < now_utc: continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        odds_data = row.get('odds', '').split(',')

        upcoming_matches.append({
            "id": match_id,
            "time": match_time.strftime('%Y-%m-%d %H:%M'),
            "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
            "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
            "handicap": format_ah_as_decimal_string_of(odds_data[2]) if len(odds_data) > 2 else "N/A",
            "goal_line": format_ah_as_decimal_string_of(odds_data[10]) if len(odds_data) > 10 else "N/A"
        })

    upcoming_matches.sort(key=lambda x: x['time'])
    return upcoming_matches

async def get_main_page_matches_async():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL_NOWGOAL, wait_until="networkidle", timeout=20000)
            await page.wait_for_selector('tr[id^="tr1_"]', timeout=10000)
            html_content = await page.content()
            return parse_main_page_matches(html_content)
        finally:
            await browser.close()

@app.route('/')
def index():
    try:
        limit = request.args.get('limit', 20, type=int)
        all_matches = asyncio.run(get_main_page_matches_async())
        filtered_matches = [m for m in all_matches if m.get('handicap') and m.get('handicap') not in ['N/A', '-']]
        matches_to_show = filtered_matches[:limit]
        return render_template('index.html', matches=matches_to_show, current_limit=limit, total_matches_found=len(filtered_matches))
    except Exception as e:
        print(f"ERROR en la ruta principal: {e}")
        return render_template('index.html', matches=[], error=f"No se pudieron cargar los partidos: {e}")

# --- RUTA PARA MOSTRAR EL ESTUDIO DETALLADO ---
@app.route('/estudio/<string:match_id>')
def mostrar_estudio(match_id):
    print(f"Recibida petición para el estudio del partido ID: {match_id}")
    datos_partido = obtener_datos_completos_partido(match_id)
    if not datos_partido or "error" in datos_partido:
        error_msg = datos_partido.get('error', 'Error desconocido al obtener los datos del partido.')
        print(f"Error al obtener datos para {match_id}: {error_msg}")
        abort(500, description=error_msg)
    return render_template('estudio.html', data=datos_partido, format_ah=format_ah_as_decimal_string_of)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)