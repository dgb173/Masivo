# modules/estudio_scraper.py
import time
import requests
import re
import math
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
import traceback

# Importaciones de Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- CONFIGURACIÓN GLOBAL ---
BASE_URL = "https://live18.nowgoal25.com"
SELENIUM_TIMEOUT_SECONDS = 15

# --- FUNCIONES DE FORMATEO Y PARSEO (IDÉNTICAS A ESTUDIO.PY) ---
def parse_ah_to_number_of(ah_line_str: str):
    if not isinstance(ah_line_str, str): return None
    s = ah_line_str.strip().replace(' ', '')
    if not s or s in ['-', '?']: return None
    original_starts_with_minus = ah_line_str.strip().startswith('-')
    try:
        if '/' in s:
            parts = s.split('/')
            if len(parts) != 2: return None
            val1, val2 = float(parts[0]), float(parts[1])
            if val1 < 0 and not parts[1].startswith('-') and val2 > 0: val2 = -abs(val2)
            elif original_starts_with_minus and val1 == 0.0 and not parts[1].startswith('-') and val2 > 0: val2 = -abs(val2)
            return (val1 + val2) / 2.0
        return float(s)
    except (ValueError, IndexError): return None

def format_ah_as_decimal_string_of(ah_line_str: str):
    numeric_value = parse_ah_to_number_of(ah_line_str)
    if numeric_value is None: return ah_line_str.strip() if isinstance(ah_line_str, str) and ah_line_str.strip() in ['-','?'] else '-'
    if numeric_value == 0.0: return "0"
    sign = -1 if numeric_value < 0 else 1
    abs_num = abs(numeric_value)
    mod_val = abs_num % 1
    if mod_val == 0.0: abs_rounded = abs_num
    elif mod_val == 0.25: abs_rounded = math.floor(abs_num) + 0.25
    elif mod_val == 0.5: abs_rounded = abs_num
    elif mod_val == 0.75: abs_rounded = math.floor(abs_num) + 0.75
    else:
        if mod_val < 0.25: abs_rounded = math.floor(abs_num)
        elif mod_val < 0.75: abs_rounded = math.floor(abs_num) + 0.5
        else: abs_rounded = math.ceil(abs_num)
    final_value = sign * abs_rounded
    if final_value == 0.0: return "0"
    if abs(final_value % 1) < 1e-9: return str(int(final_value))
    if abs(final_value - (math.floor(final_value) + 0.5)) < 1e-9: return f"{final_value:.1f}"
    return f"{final_value:.2f}"

# --- SISTEMA DE ANÁLISIS DE MERCADO (100% FIEL A ESTUDIO.PY) ---
def check_handicap_cover(resultado_raw, ah_line_num, favorite_team_name, home_team, away_team, main_home_team_name):
    try:
        goles_h, goles_a = map(int, resultado_raw.split('-'))
        if ah_line_num == 0.0:
            is_main_home_playing_home = main_home_team_name.lower() in home_team.lower()
            if (is_main_home_playing_home and goles_h > goles_a) or (not is_main_home_playing_home and goles_a > goles_h): return ("CUBIERTO", True)
            if (is_main_home_playing_home and goles_a > goles_h) or (not is_main_home_playing_home and goles_h > goles_a): return ("NO CUBIERTO", False)
            return ("PUSH", None)
        
        favorite_margin = (goles_h - goles_a) if favorite_team_name.lower() in home_team.lower() else (goles_a - goles_h)
        if favorite_margin - abs(ah_line_num) > 0.05: return ("CUBIERTO", True)
        if favorite_margin - abs(ah_line_num) < -0.05: return ("NO CUBIERTO", False)
        return ("PUSH", None)
    except (ValueError, TypeError, AttributeError): return ("indeterminado", None)

def check_goal_line_cover(resultado_raw, goal_line_num):
    try:
        total_goles = sum(map(int, resultado_raw.split('-')))
        if total_goles > goal_line_num: return ("SUPERADA (Over)", True)
        if total_goles < goal_line_num: return ("NO SUPERADA (Under)", False)
        return ("PUSH (Igual)", None)
    except (ValueError, TypeError): return ("indeterminado", None)

def analizar_precedente(precedente_data, ah_actual_num, goles_actual_num, favorito_actual_name, main_home_team_name):
    analysis_results = []
    if not isinstance(precedente_data, dict): return analysis_results
    
    details = precedente_data.get('details', precedente_data)
    if not isinstance(details, dict): return analysis_results

    res_raw, ah_raw, home, away = None, None, None, None
    if 'goles_home' in details:
        res_raw = f"{details.get('goles_home')}-{details.get('goles_away')}"
        ah_raw, home, away = details.get('handicap'), details.get('h2h_home_team_name'), details.get('h2h_away_team_name')
    else:
        res_raw, ah_raw, home, away = details.get('score_raw'), details.get('handicap_line_raw'), details.get('home_team'), details.get('away_team')

    if all(v is not None for v in [res_raw, ah_raw, home, away, ah_actual_num]) and ah_raw not in ['-', 'N/A', '?']:
        res_cover, cubierto = check_handicap_cover(res_raw, ah_actual_num, favorito_actual_name, home, away, main_home_team_name)
        color = 'green' if cubierto is True else 'red' if cubierto is False else '#6c757d'
        symbol = '✅' if cubierto is True else '❌' if cubierto is False else '🤔'
        cover_html = f"<span style='color: {color}; font-weight: bold;'>{res_cover} {symbol}</span>"
        analysis_results.append(f"Con el resultado ({res_raw.replace('-', ':')}), la línea actual se habría considerado {cover_html}.")

    if all(v is not None for v in [res_raw, goles_actual_num]) and '-' in res_raw:
        try:
            total_goles = sum(map(int, res_raw.split('-')))
            res_cover, superada = check_goal_line_cover(res_raw, goles_actual_num)
            color = 'green' if superada is True else 'red' if superada is False else '#6c757d'
            cover_html = f"<span style='color: {color}; font-weight: bold;'>{res_cover}</span>"
            analysis_results.append(f"El partido tuvo <b>{total_goles} goles</b>, por lo que la línea actual habría resultado {cover_html}.")
        except (ValueError, TypeError): pass
            
    return analysis_results
    
def _analizar_precedente_handicap(precedente_data, ah_actual_num, favorito_actual_name, main_home_team_name):
    res_raw, ah_raw = precedente_data.get('score_raw'), precedente_data.get('handicap_line_raw')
    home_team, away_team = precedente_data.get('home_team'), precedente_data.get('away_team')
    if not all([res_raw, res_raw != '?-?', ah_raw, ah_raw not in ['-', '?', 'N/A']]): return "<li><span class='ah-value'>Hándicap:</span> No hay datos suficientes en este precedente.</li>"
    
    ah_historico_num = parse_ah_to_number_of(ah_raw)
    comparativa_texto = ""
    if ah_historico_num is not None and ah_actual_num is not None:
        fav_historico = home_team if ah_historico_num > 0 else (away_team if ah_historico_num < 0 else None)
        movimiento = f"{format_ah_as_decimal_string_of(ah_raw)} → {format_ah_as_decimal_string_of(str(ah_actual_num))}"
        if (fav_historico and favorito_actual_name.lower() in fav_historico.lower()) or (not fav_historico and favorito_actual_name == "Ninguno"):
            if abs(ah_actual_num) > abs(ah_historico_num): comparativa_texto = f"El mercado lo ve <strong>más favorito</strong> (movimiento: <strong style='color: green;'>{movimiento}</strong>). "
            elif abs(ah_actual_num) < abs(ah_historico_num): comparativa_texto = f"El mercado lo ve <strong>menos favorito</strong> (movimiento: <strong style='color: orange;'>{movimiento}</strong>). "
            else: comparativa_texto = f"La línea mantiene <strong>idéntica magnitud</strong> ({movimiento}). "
        else:
            if fav_historico and favorito_actual_name != "Ninguno": comparativa_texto = f"Hubo un <strong>cambio total de favoritismo</strong> (antes '{fav_historico}', movimiento: <strong style='color: red;'>{movimiento}</strong>). "
            elif not fav_historico: comparativa_texto = f"Ahora hay un <strong>favorito claro</strong> (movimiento: <strong style='color: green;'>{movimiento}</strong>). "
            else: comparativa_texto = f"El mercado <strong>ha eliminado al favorito</strong> que era '{fav_historico}' (movimiento: <strong style='color: orange;'>{movimiento}</strong>). "
    else: comparativa_texto = f"No se pudo comparar (línea hist: {format_ah_as_decimal_string_of(ah_raw)}). "

    res_cover, cubierto = check_handicap_cover(res_raw, ah_actual_num, favorito_actual_name, home_team, away_team, main_home_team_name)
    color = 'green' if cubierto else 'red' if cubierto is False else '#6c757d'
    symbol = '✅' if cubierto else '❌' if cubierto is False else '🤔'
    cover_html = f"<span style='color: {color}; font-weight: bold;'>{res_cover} {symbol}</span>"
    return f"<li><span class='ah-value'>Hándicap:</span> {comparativa_texto}Con el resultado ({res_raw.replace('-', ':')}), la línea actual se habría considerado {cover_html}.</li>"

def _analizar_precedente_goles(precedente_data, goles_actual_num):
    res_raw = precedente_data.get('score_raw')
    if not res_raw or res_raw == '?-?' or goles_actual_num is None: return "<li><span class='score-value'>Goles:</span> No hay datos suficientes.</li>"
    try:
        total_goles = sum(map(int, res_raw.split('-')))
        res_cover, superada = check_goal_line_cover(res_raw, goles_actual_num)
        color = 'green' if superada else 'red' if superada is False else '#6c757d'
        cover_html = f"<span style='color: {color}; font-weight: bold;'>{res_cover}</span>"
        return f"<li><span class='score-value'>Goles:</span> El partido tuvo <strong>{total_goles} goles</strong>, por lo que la línea actual habría resultado {cover_html}.</li>"
    except (ValueError, TypeError): return "<li><span class='score-value'>Goles:</span> No se pudo procesar el resultado del precedente.</li>"

def generar_analisis_completo_mercado(main_odds, h2h_data, home_name, away_name):
    ah_actual_str = format_ah_as_decimal_string_of(main_odds.get('ah_linea_raw', '-'))
    ah_actual_num = parse_ah_to_number_of(ah_actual_str)
    goles_actual_num = parse_ah_to_number_of(main_odds.get('goals_linea_raw', '-'))
    if ah_actual_num is None or goles_actual_num is None: return ""

    favorito_name, favorito_html = "Ninguno", "Ninguno (línea en 0)"
    if ah_actual_num < 0: favorito_name, favorito_html = away_name, f"<span class='away-color'>{away_name}</span>"
    elif ah_actual_num > 0: favorito_name, favorito_html = home_name, f"<span class='home-color'>{home_name}</span>"
    
    precedente_estadio = {'score_raw': h2h_data.get('res1_raw'), 'handicap_line_raw': h2h_data.get('ah1'), 'home_team': home_name, 'away_team': away_name, 'match_id': h2h_data.get('match1_id')}
    sintesis_ah_estadio = _analizar_precedente_handicap(precedente_estadio, ah_actual_num, favorito_name, home_name)
    sintesis_goles_estadio = _analizar_precedente_goles(precedente_estadio, goles_actual_num)
    
    analisis_general_html = ""
    if precedente_estadio.get('match_id') and precedente_estadio.get('match_id') == h2h_data.get('match6_id'):
        analisis_general_html = "<p class='mt-2 mb-0'><small><em>El H2H general más reciente es el mismo partido en este estadio.</em></small></p>"
    else:
        precedente_general = {'score_raw': h2h_data.get('res6_raw'), 'handicap_line_raw': h2h_data.get('ah6'), 'home_team': h2h_data.get('h2h_gen_home'), 'away_team': h2h_data.get('h2h_gen_away')}
        sintesis_ah_general = _analizar_precedente_handicap(precedente_general, ah_actual_num, favorito_name, home_name)
        sintesis_goles_general = _analizar_precedente_goles(precedente_general, goles_actual_num)
        analisis_general_html = f"<h6>✈️ Análisis del H2H General Más Reciente</h6><ul>{sintesis_ah_general}{sintesis_goles_general}</ul>"

    return f"""<div class="analysis-box" style="font-size: 0.9em; background-color: #f0f2f6; border-left-color: #1E90FF;"><p class='mb-2'><strong>📊 Análisis de Mercado vs. Histórico H2H</strong><br><small class='text-muted'>Líneas actuales: AH {ah_actual_str} / Goles {format_ah_as_decimal_string_of(main_odds.get('goals_linea_raw'))} | Favorito: {favorito_html}</small></p><h6>🏟️ Análisis del Precedente en Este Estadio</h6><ul>{sintesis_ah_estadio}{sintesis_goles_estadio}</ul>{analisis_general_html}</div>"""

# --- FUNCIONES DE EXTRACCIÓN (100% PORTADAS Y MEJORADAS) ---
def _get_selenium_driver():
    options = ChromeOptions(); options.add_argument("--headless"); options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-gpu"); options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36"); options.add_argument('--blink-settings=imagesEnabled=false')
    try: return webdriver.Chrome(options=options)
    except WebDriverException as e: print(f"Error inicializando Selenium: {e}"); return None

def _create_requests_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36"})
    return session

def get_match_details_from_row_of(row, score_class_selector):
    try:
        cells = row.find_all('td')
        if len(cells) < 12: return None
        home_team = (cells[2].find('a') or cells[2]).get_text(strip=True)
        away_team = (cells[4].find('a') or cells[4]).get_text(strip=True)
        score_cell = cells[3]
        score_span = score_cell.find('span', class_=lambda c: isinstance(c, str) and score_class_selector in c)
        score_raw_text = (score_span.get_text(strip=True) if score_span else score_cell.get_text(strip=True)) or ''
        m = re.search(r'(\d+)\s*-\s*(\d+)', score_raw_text)
        score_raw, score_fmt = (f"{m.group(1)}-{m.group(2)}", f"{m.group(1)}:{m.group(2)}") if m else ('?-?', '?:?')
        ah_cell = cells[11]
        ah_line_raw = (ah_cell.get('data-o') or ah_cell.text).strip()
        date_span = cells[1].find('span', attrs={'name': 'timeData'})
        return {'home_team': home_team, 'away_team': away_team, 'score': score_fmt, 'score_raw': score_raw, 'handicap_line_raw': ah_line_raw or '-', 'match_id': row.get('index'), 'league_id_hist': row.get('name'), 'date': date_span.get_text(strip=True) if date_span else ''}
    except Exception: return None

# ¡FUNCIÓN CLAVE CORREGIDA PARA EL IDIOMA!
def get_match_progression_stats_data(session, match_id):
    if not (match_id and match_id.isdigit()): return pd.DataFrame(columns=['Casa', 'Fuera'])
    try:
        url = f"https://live18.nowgoal25.com/match/live-{match_id}"
        soup = BeautifulSoup(session.get(url, timeout=10).text, 'lxml')
        
        # Mapeo de posibles nombres de estadísticas a un nombre canónico en inglés
        stats_map = {
            "Shots": "Shots", "Disparos": "Shots",
            "Shots on Goal": "Shots on Goal", "Disparos a Puerta": "Shots on Goal",
            "Attacks": "Attacks", "Ataques": "Attacks",
            "Dangerous Attacks": "Dangerous Attacks", "Ataques Peligrosos": "Dangerous Attacks"
        }
        # Diccionario para guardar los resultados con los nombres canónicos
        stats_results = { "Shots": "-", "Shots on Goal": "-", "Attacks": "-", "Dangerous Attacks": "-" }

        if ul := soup.select_one('div#teamTechDiv_detail ul.stat'):
            for li in ul.find_all('li'):
                title_span = li.find('span', class_='stat-title')
                if title_span:
                    title_text = title_span.text.strip()
                    if title_text in stats_map:
                        canonical_key = stats_map[title_text]
                        values = [v.text.strip() for v in li.find_all('span', class_='stat-c')]
                        if len(values) == 2:
                            stats_results[canonical_key] = {"Home": values[0], "Away": values[1]}
        
        df = pd.DataFrame([
            {"Estadistica_EN": key, "Casa": val.get('Home', '-'), "Fuera": val.get('Away', '-')}
            for key, val in stats_results.items() if isinstance(val, dict)
        ])
        
        return df.set_index("Estadistica_EN") if not df.empty else pd.DataFrame(columns=['Casa', 'Fuera'])
    except requests.RequestException:
        return pd.DataFrame(columns=['Casa', 'Fuera'])

def get_rival_h2h_info(soup, table_id, league_id):
    if not (table := soup.find("table", id=table_id)): return (None, None, None)
    for row in table.find_all("tr", {"vs": "1"}):
        if league_id and row.get("name") != str(league_id): continue
        if (key_id := row.get("index")) and (links := row.find_all("a", onclick=True)):
            rival_idx = 1 if table_id == "table_v1" else 0
            if len(links) > rival_idx and (m := re.search(r"team\((\d+)\)", links[rival_idx].get("onclick", ""))):
                return key_id, m.group(1), links[rival_idx].text.strip()
    return (None, None, None)

def get_h2h_details_for_original_logic_of(driver, key_match_id, rival_a_id, rival_b_id):
    if not all([driver, key_match_id, rival_a_id, rival_b_id]): return {"status": "error", "resultado": "Datos de rivales incompletos."}
    try:
        driver.get(f"{BASE_URL}/match/h2h-{key_match_id}"); WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "table_v2")))
        soup = BeautifulSoup(driver.page_source, "lxml")
        table = soup.find("table", id="table_v2")
        if not table: return {"status": "error", "resultado": "Tabla de H2H de rival no encontrada."}
        for row in table.find_all("tr", id=re.compile(r"tr2_\d+")):
            links = row.find_all("a", onclick=True)
            if len(links) < 2: continue
            h_m, a_m = re.search(r"team\((\d+)\)", links[0].get('onclick','')), re.search(r"team\((\d+)\)", links[1].get('onclick',''))
            if h_m and a_m and {h_m.group(1), a_m.group(1)} == {str(rival_a_id), str(rival_b_id)}:
                if (score := row.find("span", class_="fscore_2")) and '-' in score.text:
                    g_h, g_a = score.text.strip().split('(')[0].strip().split('-')
                    ah = (row.find_all('td')[11].get("data-o") or row.find_all('td')[11].text).strip()
                    return {"status": "found", "goles_home": g_h, "goles_away": g_a, "handicap": ah, "match_id": row.get('index'), "h2h_home_team_name": links[0].text.strip(), "h2h_away_team_name": links[1].text.strip()}
    except Exception as e: return {"status": "error", "resultado": f"Error en Selenium: {type(e).__name__}"}
    return {"status": "not_found", "resultado": "H2H directo no encontrado."}

def get_team_league_info_from_script_of(soup):
    if (tag := soup.find("script", string=re.compile(r"var _matchInfo ="))) and tag.string:
        def find(p): m = re.search(p, tag.string); return (m.group(1).replace("\\'", "'") if m else None)
        return find(r"hId:\s*parseInt\('(\d+)'\)"), find(r"gId:\s*parseInt\('(\d+)'\)"), find(r"sclassId:\s*parseInt\('(\d+)'\)"), find(r"hName:\s*'([^']*)'") or "Local", find(r"gName:\s*'([^']*)'") or "Visitante"
    return None, None, None, "Local", "Visitante"

def _parse_date_ddmmyyyy(d):
    m = re.search(r'(\d{2})-(\d{2})-(\d{4})', d or ''); return (int(m[3]), int(m[2]), int(m[1])) if m else (1900, 1, 1)

def extract_last_match(soup, table_id, team_name, league_id, is_home):
    def find(league_filter):
        matches = []
        if table := soup.find("table", id=table_id):
            score_class = 'fscore_1' if table_id == 'table_v1' else 'fscore_2'
            for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
                if not (d := get_match_details_from_row_of(row, score_class)): continue
                if league_filter and d.get("league_id_hist") != str(league_filter): continue
                is_team_home = team_name.lower() in d.get('home_team', '').lower()
                is_team_away = team_name.lower() in d.get('away_team', '').lower()
                if (is_home and is_team_home) or (not is_home and is_team_away): matches.append(d)
        return matches
    candidates = find(league_id) or find(None)
    return sorted(candidates, key=lambda x: _parse_date_ddmmyyyy(x.get('date')), reverse=True)[0] if candidates else None

def extract_bet365_initial_odds_of(soup):
    odds = {"ah_linea_raw": "N/A", "goals_linea_raw": "N/A"}
    if (row := soup.select_one("tr#tr_o_1_8[name='earlyOdds'], tr#tr_o_1_31[name='earlyOdds']")) and len(tds := row.find_all("td")) > 9:
        odds["ah_linea_raw"], odds["goals_linea_raw"] = (tds[3].get("data-o") or tds[3].text).strip(), (tds[9].get("data-o") or tds[9].text).strip()
    return odds

def extract_standings_data_from_h2h_page_of(soup, team_name):
    data = {"name": team_name, "ranking": "N/A"};
    if not (s_section := soup.find("div", id="porletP4")): return data
    home_div_text = (s_section.find("div", class_="home-div") or BeautifulSoup("", "lxml")).get_text(strip=True).lower()
    guest_div_text = (s_section.find("div", class_="guest-div") or BeautifulSoup("", "lxml")).get_text(strip=True).lower()
    div = s_section.find("div", class_="home-div") if team_name.lower() in home_div_text else (s_section.find("div", class_="guest-div") if team_name.lower() in guest_div_text else None)
    if div and (table := div.find("table")):
        is_home = "home" in div.get('class', [])
        data["specific_type"] = "Est. como Local" if is_home else "Est. como Visitante"
        if (a := table.find("a")) and (m := re.search(r'\[.*?-(\d+)\]', a.text)): data["ranking"] = m.group(1)
        ft_section = False
        for row in table.find_all("tr", align="center"):
            if th := row.find("th"): ft_section = "FT" in th.text; continue
            if ft_section and len(cells := row.find_all("td")) >= 7:
                row_type, stats = cells[0].text.strip(), [c.text.strip() for c in cells[1:7]]
                prefix = "total" if row_type == "Total" else "specific" if row_type == ("Home" if is_home else "Away") else None
                if prefix: data.update({f"{prefix}_{k}": v for k, v in zip(["pj", "v", "e", "d", "gf", "gc"], stats)})
    return data

def extract_over_under_stats_from_div_of(soup, team_type):
    default = {"total": 0, "over_pct": 0, "under_pct": 0, "push_pct": 0}; table_id = "table_v1" if team_type == 'home' else "table_v2"
    if (table := soup.find("table", id=table_id)) and (y_bar := table.find("ul", class_="y-bar")):
        for group in y_bar.find_all("li", class_="group"):
            if "Over/Under Odds" in group.text:
                try:
                    total = int(re.search(r'\((\d+)', group.find("div", class_="tit").text).group(1))
                    vals = [float(v.text.strip('%')) for v in group.find_all("span", class_="value")]
                    return {"over_pct": vals[0], "push_pct": vals[1], "under_pct": vals[2], "total": total} if len(vals) == 3 else default
                except (ValueError, TypeError, AttributeError): pass
    return default

def extract_h2h_data_of(soup, home_name, away_name):
    results = {'res1': '?:?', 'match1_id': None, 'res6': '?:?', 'match6_id': None, 'ah1': '-', 'ah6': '-', 'h2h_gen_home': 'N/A', 'h2h_gen_away': 'N/A', 'res1_raw': '?-?', 'res6_raw': '?-?'}
    if table := soup.find("table", id="table_v3"):
        matches = sorted([d for r in table.find_all("tr") if (d := get_match_details_from_row_of(r, 'fscore_3'))], key=lambda x: _parse_date_ddmmyyyy(x.get('date')), reverse=True)
        if matches:
            results.update({k: matches[0][v] for k, v in {'res6': 'score', 'res6_raw': 'score_raw', 'ah6': 'handicap_line_raw', 'match6_id': 'match_id', 'h2h_gen_home': 'home_team', 'h2h_gen_away': 'away_team'}.items()})
            for m in matches:
                if m['home_team'].lower() == home_name.lower() and m['away_team'].lower() == away_name.lower():
                    results.update({k: m[v] for k, v in {'res1': 'score', 'res1_raw': 'score_raw', 'ah1': 'handicap_line_raw', 'match1_id': 'match_id'}.items()}); break
    return results

def extract_comparative_match_of(soup, table_id, main_team, opponent, league_id):
    if not all([opponent, opponent != "N/A", main_team, table := soup.find("table", id=table_id)]): return None
    score_class = 'fscore_1' if table_id == 'table_v1' else 'fscore_2'
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if d := get_match_details_from_row_of(row, score_class):
            if league_id and d.get('league_id_hist') and d.get('league_id_hist') != str(league_id): continue
            h, a = d.get('home_team','').lower(), d.get('away_team','').lower()
            if {main_team.lower(), opponent.lower()} == {h, a}:
                d['localia'] = 'H' if main_team.lower() == h else 'A'; return d
    return None

# --- FUNCIÓN PRINCIPAL ORQUESTADORA ---
def obtener_datos_completos_partido(match_id: str) -> dict:
    if not (match_id and match_id.isdigit()): return {"error": "ID de partido no válido."}
    driver = _get_selenium_driver()
    if not driver: return {"error": "No se pudo inicializar el navegador."}
    
    session = _create_requests_session()
    all_data, start_time = {}, time.time()
    
    try:
        driver.get(f"{BASE_URL}/match/h2h-{match_id}"); WebDriverWait(driver, SELENIUM_TIMEOUT_SECONDS).until(EC.presence_of_element_located((By.ID, "table_v1")))
        for select_id in ["hSelect_1", "hSelect_2", "hSelect_3"]:
            try: Select(WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.ID, select_id)))).select_by_value("8"); time.sleep(0.1)
            except TimeoutException: pass
        soup = BeautifulSoup(driver.page_source, "lxml")

        _, _, league_id, home_name, away_name = get_team_league_info_from_script_of(soup)
        all_data.update({"home_name": home_name, "away_name": away_name})
        
        main_odds = extract_bet365_initial_odds_of(soup)
        main_odds['ah_linea'], main_odds['goals_linea'] = format_ah_as_decimal_string_of(main_odds.get('ah_linea_raw')), format_ah_as_decimal_string_of(main_odds.get('goals_linea_raw'))
        ah_num, goles_num = parse_ah_to_number_of(main_odds.get('ah_linea_raw')), parse_ah_to_number_of(main_odds.get('goals_linea_raw'))
        fav_name = away_name if ah_num is not None and ah_num < 0 else (home_name if ah_num is not None and ah_num > 0 else "Ninguno")
        all_data['main_match_odds'] = main_odds

        key_match_id_a, rival_a_id, _ = get_rival_h2h_info(soup, "table_v1", league_id)
        _, rival_b_id, _ = get_rival_h2h_info(soup, "table_v2", league_id)

        with ThreadPoolExecutor(max_workers=8) as executor:
            f_h_stand = executor.submit(extract_standings_data_from_h2h_page_of, soup, home_name)
            f_a_stand = executor.submit(extract_standings_data_from_h2h_page_of, soup, away_name)
            f_h_ou = executor.submit(extract_over_under_stats_from_div_of, soup, 'home')
            f_a_ou = executor.submit(extract_over_under_stats_from_div_of, soup, 'away')
            f_last_h = executor.submit(extract_last_match, soup, "table_v1", home_name, league_id, True)
            f_last_a = executor.submit(extract_last_match, soup, "table_v2", away_name, league_id, False)
            f_h2h = executor.submit(extract_h2h_data_of, soup, home_name, away_name)
            f_h2h_col3 = executor.submit(get_h2h_details_for_original_logic_of, driver, key_match_id_a, rival_a_id, rival_b_id)
            
            last_home, last_away, h2h_data = f_last_h.result(), f_last_a.result(), f_h2h.result()
            comp_L_vs_UV_A = extract_comparative_match_of(soup, "table_v1", home_name, (last_away or {}).get('home_team'), league_id)
            comp_V_vs_UL_H = extract_comparative_match_of(soup, "table_v2", away_name, (last_home or {}).get('away_team'), league_id)
            all_data.update({k: f.result() for k, f in {'home_standings': f_h_stand, 'away_standings': f_a_stand, 'home_ou_stats': f_h_ou, 'away_ou_stats': f_a_ou, 'h2h_col3_raw': f_h2h_col3}.items()})

        partidos = {"last_home_match": last_home, "last_away_match": last_away, "h2h_col3": all_data.get('h2h_col3_raw') if all_data.get('h2h_col3_raw', {}).get('status') == 'found' else None, "comp_L_vs_UV_A": comp_L_vs_UV_A, "comp_V_vs_UL_H": comp_V_vs_UL_H, "h2h_stadium": h2h_data if h2h_data.get('res1') != '?:?' else None, "h2h_general": h2h_data if h2h_data.get('res6') != '?:?' else None}
        
        with ThreadPoolExecutor(max_workers=len(partidos)) as executor:
            future_stats = {executor.submit(get_match_progression_stats_data, session, (v or {}).get('match_id')): k for k, v in partidos.items() if v}
            for future in future_stats:
                key, details = future_stats[future], partidos[future_stats[future]]
                all_data[key] = {"details": details, "stats": future.result(), "analysis": analizar_precedente({"details": details}, ah_num, goles_num, fav_name, home_name)}
        
        for key in partidos.keys():
            if key not in all_data: all_data[key] = {"details": None, "stats": None, "analysis": []}
        
        all_data['market_analysis_html'] = generar_analisis_completo_mercado(main_odds, h2h_data, home_name, away_name)

    except Exception as e:
        print(f"Error crítico durante el scraping para el ID {match_id}: {e}")
        traceback.print_exc()
        return {"error": f"Ocurrió un error al procesar el partido: {e}"}
    finally:
        if driver: driver.quit()
        print(f"Análisis para ID {match_id} completado en {time.time() - start_time:.2f} segundos.")
    return all_data
