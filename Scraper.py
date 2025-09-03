# -*- coding: utf-8 -*-
# ==============================================================================
#  SCRIPT UNIFICADO PARA SCRAPING - Versión 5.3 (Adaptado para Ejecución Local)
# ==============================================================================

# --- 1. IMPORTACIONES ---
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import gspread
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
import os
import psutil

# --- 2. CONFIGURACIÓN GLOBAL ---
print("--- [Paso 1/7] Configurando el script... ---")

# -- Credenciales y Google Sheets --
NOMBRE_SHEET = "Datos" # Nombre de tu Google Sheet
NOMBRE_HOJA_NEG_CERO = "Visitantes" # Hoja para partidos con AH del visitante o 0
NOMBRE_HOJA_POSITIVOS = "Locales" # Hoja para partidos con AH del local

# -- Rangos de Partidos a Extraer --
EXTRACTION_RANGES = [
    {'start_id': 2696131, 'end_id': 2696130, 'label': 'Test Match Melbourne'}, # ID del ejemplo
    {'start_id': 2610938, 'end_id': 2610937, 'label': 'Test Match St.George'},
]

# -- Parámetros de Rendimiento --
MAX_WORKERS = 4
SELENIUM_TIMEOUT = 15
BATCH_SIZE = 150
API_PAUSE = 0.3
WORKER_START_DELAY = random.uniform(0.3, 0.8)

# -- Columnas Finales --
COLS = ["AH_H2H_V", "AH_Act", "Res_H2H_V", "AH_L_H", "Res_L_H",
        "AH_V_A", "Res_V_A", "AH_H2H_G", "Res_H2H_G",
        "L_vs_UV_A", "V_vs_UL_H", "Regla_3",
        "Stats_L", "Stats_V",
        "Fin", "G_i", "match_id"]

print("✅ Configuración cargada.\n")

# --- 3. MANEJO DE CREDENCIALES ---
print("--- [Paso 2/7] Gestionando credenciales de Google... ---")
# El script buscará el archivo en la misma carpeta donde lo ejecutes.
CREDENTIALS_FILENAME = "google_credentials.json"
if not os.path.exists(CREDENTIALS_FILENAME):
    print(f"❌ Error: Archivo de credenciales '{CREDENTIALS_FILENAME}' no encontrado.")
    print("   Asegúrate de que el archivo .json esté en la misma carpeta que este script.")
    exit() # Detiene la ejecución si no encuentra el archivo
else:
    print(f"✅ Archivo de credenciales encontrado.\n")


# --- 4. CONEXIÓN A GOOGLE SHEETS ---
print(f"--- [Paso 3/7] Conectando a Google Sheet '{NOMBRE_SHEET}'... ---")
try:
    gc = gspread.service_account(filename=CREDENTIALS_FILENAME)
    sh = gc.open(NOMBRE_SHEET)
    print(f"✅ Conexión exitosa.\n")
except Exception as e:
    print(f"❌ Error crítico conectando a Google Sheets: {e}"); exit()


# --- 5. FUNCIONES HELPER Y DE LÓGICA AVANZADA ---

def get_chrome_options():
    chrome_opts = Options()
    chrome_opts.add_argument('--headless')
    chrome_opts.add_argument('--no-sandbox')
    chrome_opts.add_argument('--disable-dev-shm-usage')
    chrome_opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    chrome_opts.add_argument('--disable-blink-features=AutomationControlled')
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument('--blink-settings=imagesEnabled=false')
    return chrome_opts

# (El resto de funciones helper no necesitan cambios)
def parse_ah_to_number(ah_line_str: str):
    if not isinstance(ah_line_str, str): return None
    s = ah_line_str.strip().replace(' ', '')
    if not s or s in ['-', '?']: return None
    try:
        if '/' in s:
            parts = s.split('/')
            return (float(parts[0]) + float(parts[1])) / 2.0
        return float(s)
    except (ValueError, IndexError): return None

def format_ah_as_decimal_string(ah_line_str: str):
    numeric_value = parse_ah_to_number(ah_line_str)
    if numeric_value is None: return ah_line_str.strip() if isinstance(ah_line_str, str) else '-'
    if numeric_value == 0.0: return "0"
    if abs(numeric_value % 0.5) == 0.25: return f"{numeric_value:.2f}"
    return f"{numeric_value:.1f}"

def get_match_details_from_row(row_element, score_class_selector='score'):
    try:
        cells = row_element.find_all('td')
        if len(cells) < 12: return None
        home = (cells[2].find('a') or cells[2]).text.strip()
        away = (cells[4].find('a') or cells[4]).text.strip()
        score_span = cells[3].find('span', class_=lambda x: x and score_class_selector in x)
        score_raw_text = (score_span or cells[3]).text.strip()
        score_m = re.match(r'(\d+-\d+)', score_raw_text)
        score_raw = score_m.group(1) if score_m else '?-?'
        ah_line_raw_text = (cells[11].get("data-o") or cells[11].text).strip()
        date_span = cells[1].find('span', attrs={'name': 'timeData'})
        return {'home': home, 'away': away, 'score': score_raw.replace('-', '*'), 'score_raw': score_raw,
                'ahLine': format_ah_as_decimal_string(ah_line_raw_text), 'ahLine_raw': ah_line_raw_text,
                'date': (date_span.text.strip() if date_span else ''), 'matchIndex': row_element.get('index'),
                'league_id_hist': row_element.get('name')}
    except Exception: return None

def extract_team_stats_from_summary(soup_obj, table_selector, is_home_team):
    try:
        table = soup_obj.select_one(table_selector)
        rows = table.find_all('tr')
        loc_aw_char = "L" if is_home_team else "V"
        total_cells, loc_aw_cells = rows[2].find_all('td'), rows[4].find_all('td')
        return (f"🏆Rk:{total_cells[8].text.strip()} {'🏠Home' if is_home_team else '✈️Away'}\n"
                f"🌍T:{total_cells[1].text.strip()}|{total_cells[2].text.strip()}/{total_cells[3].text.strip()}/{total_cells[4].text.strip()}|{total_cells[5].text.strip()}-{total_cells[6].text.strip()}\n"
                f"🏡{loc_aw_char}:{loc_aw_cells[1].text.strip()}|{loc_aw_cells[2].text.strip()}/{loc_aw_cells[3].text.strip()}/{loc_aw_cells[4].text.strip()}|{loc_aw_cells[5].text.strip()}-{loc_aw_cells[6].text.strip()}")
    except (IndexError, AttributeError): return f"Stats {loc_aw_char}: N/A"

def get_team_league_info_from_script(soup):
    script_tag = soup.find("script", string=re.compile(r"var _matchInfo ="))
    if not script_tag: return (None,) * 6
    content = script_tag.string
    def find_val(pattern):
        m = re.search(pattern, content)
        return m.group(1).strip() if m else None
    return (find_val(r"hId:\s*parseInt\('(\d+)'\)"), find_val(r"gId:\s*parseInt\('(\d+)'\)"),
            find_val(r"sclassId:\s*parseInt\('(\d+)'\)"), find_val(r"hName:\s*'([^']*)'"),
            find_val(r"gName:\s*'([^']*)'"), find_val(r"lName:\s*'([^']*)'"))

def _parse_date_ddmmyyyy(d: str):
    m = re.search(r'(\d{2})-(\d{2})-(\d{4})', d or '')
    return (int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else (1900, 1, 1)

def extract_last_match_in_league(soup, table_id, team_name, league_id, is_home_game):
    if not (table := soup.find("table", id=table_id)): return None
    matches = []
    score_selector = 'fscore_1' if is_home_game else 'fscore_2'
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if (details := get_match_details_from_row(row, score_selector)) and details.get('league_id_hist') == league_id:
            team_key = 'home' if is_home_game else 'away'
            if team_name.lower() in details.get(team_key, '').lower():
                matches.append(details)
    if not matches: return None
    matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date')), reverse=True)
    return matches[0]

def extract_comparative_match(soup, table_id, main_team, opponent, league_id):
    if not opponent or not (table := soup.find("table", id=table_id)): return "-"
    selector = 'fscore_1' if table_id == "table_v1" else 'fscore_2'
    for row in table.find_all("tr"):
        if (details := get_match_details_from_row(row, selector)) and details.get('league_id_hist') == league_id:
            h, a = details.get('home','').lower(), details.get('away','').lower()
            if {main_team.lower(), opponent.lower()} == {h, a}:
                localia = 'H' if main_team.lower() == h else 'A'
                return f"{details.get('score', '?*?')}/{details.get('ahLine', '-')} {localia}"
    return "-"

def get_key_and_rival_ids(soup, table_id: str):
    if not soup or not (table := soup.find("table", id=table_id)):
        return None, None, None
    for row in table.find_all("tr", id=re.compile(rf"tr{table_id[-1]}_\d+")):
        if row.get("vs") == "1" and (key_id := row.get("index")):
            link_index = 1 if table_id == "table_v1" else 0
            onclicks = row.find_all("a", onclick=True)
            if len(onclicks) > link_index and (rival_tag := onclicks[link_index]):
                if rival_id_match := re.search(r"team\((\d+)\)", rival_tag.get("onclick", "")):
                    return key_id, rival_id_match.group(1), rival_tag.text.strip()
    return None, None, None

def get_col3_h2h_details_from_new_page(driver, base_url, key_match_id, rival_a_id, rival_b_id):
    if not all([key_match_id, rival_a_id, rival_b_id]):
        return {"status": "error", "reason": "Datos de entrada incompletos"}

    url = f"{base_url}/match/h2h-{key_match_id}"
    try:
        driver.get(url)
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.ID, "table_v2")))
        try: 
            select = Select(WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "hSelect_2"))))
            select.select_by_value("8")
            time.sleep(0.5)
        except TimeoutException:
            pass
        
        soup = BeautifulSoup(driver.page_source, "lxml")
        table = soup.find("table", id="table_v2")
        if not table:
            return {"status": "error", "reason": "No se encontró table_v2 en la página H2H."}

        for row in table.find_all("tr", id=re.compile(r"tr2_\d+")):
            links = row.find_all("a", onclick=True)
            if len(links) < 2: continue
            
            h_id_m = re.search(r"team\((\d+)\)", links[0].get("onclick", ""))
            a_id_m = re.search(r"team\((\d+)\)", links[1].get("onclick", ""))
            if not (h_id_m and a_id_m): continue
            
            if {h_id_m.group(1), a_id_m.group(1)} == {str(rival_a_id), str(rival_b_id)}:
                if not (score_span := row.find("span", class_="fscore_2")) or "-" not in score_span.text:
                    continue
                
                score = score_span.text.strip().split("(")[0].strip()
                tds = row.find_all("td")
                handicap_raw = "-"
                if len(tds) > 11:
                    cell = tds[11]
                    handicap_raw = (cell.get("data-o") or cell.text).strip() or "-"
                
                return { "status": "found", "score": score.replace('-', '*'), "handicap": handicap_raw, "home_team": links[0].text.strip() }
        return {"status": "not_found"}
    except Exception as e:
        return {"status": "error", "reason": str(e)}

def format_col3_h2h_rivals(h2h_details, rival_local_name):
    if not h2h_details or h2h_details.get("status") != "found": return "-"
    score = h2h_details.get('score', '?*?')
    ah_raw = h2h_details.get('handicap', '-')
    ah = format_ah_as_decimal_string(ah_raw)
    h2h_home_team = h2h_details.get('home_team', '')
    localia_str = "(RL-RV)" if rival_local_name and rival_local_name.lower() in h2h_home_team.lower() else "(RV-RL)"
    return f"{score}/{ah} {localia_str}"

# --- 6. WORKER PRINCIPAL DE EXTRACCIÓN ---
def extract_match_worker(mid):
    # ¡CAMBIO IMPORTANTE! Esta sección ahora busca chromedriver.exe en la misma carpeta.
    service = ChromeService(executable_path="chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=get_chrome_options())
    
    BASE_URL = "https://live18.nowgoal25.com"
    original_url = f"{BASE_URL}/match/h2h-{mid}"
    try:
        driver.get(original_url)
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.ID, "table_v1")))
        for select_id in ["hSelect_1", "hSelect_2", "hSelect_3"]:
            try: Select(WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.ID, select_id)))).select_by_value("8")
            except TimeoutException: continue
        time.sleep(0.5)
        soup_main = BeautifulSoup(driver.page_source, 'lxml')
        if "match not found" in driver.page_source.lower(): return mid, 'not_found', None

        home_id, away_id, league_id, home_name, away_name, _ = get_team_league_info_from_script(soup_main)
        if not all([home_id, away_id, league_id, home_name, away_name]): return mid, 'parse_error', (original_url, "Missing base IDs or names")

        odds_row = soup_main.select_one('#tr_o_1_8[name="earlyOdds"], #tr_o_1_31[name="earlyOdds"]')
        ah_raw = (odds_row.select_one('td:nth-of-type(4)').get("data-o") or odds_row.select_one('td:nth-of-type(4)').text).strip() if odds_row else '?'
        goals_raw = (odds_row.select_one('td:nth-of-type(10)').get("data-o") or odds_row.select_one('td:nth-of-type(10)').text).strip() if odds_row else '?'
        ah_curr_str, goals_curr_str = format_ah_as_decimal_string(ah_raw), format_ah_as_decimal_string(goals_raw)
        ah_curr_num = parse_ah_to_number(ah_raw)
        
        scores = soup_main.select('#mScore .end .score')
        finalScoreFmt = f"{scores[0].text.strip()}*{scores[1].text.strip()}" if len(scores) == 2 else "?*?"
        
        h2h_rows = soup_main.select('#table_v3 tr[id^="tr3_"]')
        h2h_matches = [d for r in h2h_rows if (d := get_match_details_from_row(r, 'fscore_3')) and d.get('league_id_hist') == league_id]
        h2h_matches.sort(key=lambda x: _parse_date_ddmmyyyy(x.get('date')), reverse=True)
        ah1, res1, ah6, res6 = '-', '?*?', '-', '?*?'
        if h2h_matches:
            ah6, res6 = h2h_matches[0]['ahLine'], h2h_matches[0]['score']
            for m in h2h_matches:
                if m['home'].lower() == home_name.lower():
                    ah1, res1 = m['ahLine'], m['score']; break
        
        last_home_match = extract_last_match_in_league(soup_main, "table_v1", home_name, league_id, True)
        last_away_match = extract_last_match_in_league(soup_main, "table_v2", away_name, league_id, False)
        ah4, res4 = (last_home_match['ahLine'], last_home_match['score']) if last_home_match else ('-', '?*?')
        ah5, res5 = (last_away_match['ahLine'], last_away_match['score']) if last_away_match else ('-', '?*?')
        
        rival_of_last_home = (last_home_match or {}).get('away')
        rival_of_last_away = (last_away_match or {}).get('home')
        comp7 = extract_comparative_match(soup_main, "table_v1", home_name, rival_of_last_away, league_id)
        comp8 = extract_comparative_match(soup_main, "table_v2", away_name, rival_of_last_home, league_id)
        
        key_id_a, rival_a_id, rival_a_name = get_key_and_rival_ids(soup_main, "table_v1")
        _, rival_b_id, _ = get_key_and_rival_ids(soup_main, "table_v2")

        details_h2h_col3 = get_col3_h2h_details_from_new_page(driver, BASE_URL, key_id_a, rival_a_id, rival_b_id)
        regla3 = format_col3_h2h_rivals(details_h2h_col3, rival_a_name)
        
        driver.get(original_url)
        WebDriverWait(driver, SELENIUM_TIMEOUT).until(EC.presence_of_element_located((By.ID, "table_v1")))
        
        localStatsStr = extract_team_stats_from_summary(soup_main, 'table.team-table-home', True)
        visitorStatsStr = extract_team_stats_from_summary(soup_main, 'table.team-table-guest', False)
        
        final_row_data = [ah1, ah_curr_str, res1, ah4, res4, ah5, res5, ah6, res6, comp7, comp8, regla3, localStatsStr, visitorStatsStr, finalScoreFmt, goals_curr_str, str(mid)]
        
        formatted_row = []
        for item in final_row_data:
            s_item = str(item)
            try:
                float(s_item.replace(',', '.'))
                formatted_row.append("'" + s_item.replace('.', ','))
            except (ValueError, AttributeError):
                formatted_row.append(s_item)

        return mid, 'ok', (formatted_row, ah_curr_num)

    except Exception as e:
        return mid, 'parse_error', (original_url, f"{type(e).__name__}: {str(e)}")
    finally:
        if driver: driver.quit()


# --- 7. BUCLE PRINCIPAL Y RESUMEN ---
def worker_task(mid):
    time.sleep(WORKER_START_DELAY)
    return extract_match_worker(mid)

def upload_data_to_sheet(worksheet_name, data_rows, columns_list, sheet_handle):
    if not data_rows:
        print(f"  ✅ No hay datos nuevos para subir a '{worksheet_name}'.")
        return True
    print(f"\n--- Subiendo {len(data_rows)} filas a la hoja '{worksheet_name}'... ---")
    try:
        ws = sheet_handle.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Hoja '{worksheet_name}' no encontrada. Creando...")
        ws = sheet_handle.add_worksheet(title=worksheet_name, rows=len(data_rows) + 100, cols=len(columns_list))
    
    header_in_sheet = ws.get('A1:Z1')
    if not header_in_sheet or header_in_sheet[0] != columns_list:
        print("  Escribiendo encabezado...")
        ws.update('A1', [columns_list], value_input_option='USER_ENTERED')
        time.sleep(API_PAUSE)

    start_row = len(ws.get_all_values()) + 1
    num_batches = math.ceil(len(data_rows) / BATCH_SIZE)
    print(f"  Subiendo en {num_batches} lotes...")
    for i in range(num_batches):
        start_index = i * BATCH_SIZE
        end_index = start_index + BATCH_SIZE
        batch_data = data_rows[start_index:end_index]
        target_range = f'A{start_row + start_index}'
        print(f"    Lote {i+1}/{num_batches}: Subiendo {len(batch_data)} filas...", end="")
        try:
            ws.update(target_range, batch_data, value_input_option='USER_ENTERED')
            print(" OK.")
            time.sleep(API_PAUSE)
        except gspread.exceptions.APIError as e:
            print(f" ❌ ERROR API. Esperando 60s. Mensaje: {e}")
            time.sleep(60)
            try:
                ws.update(target_range, batch_data, value_input_option='USER_ENTERED')
                print(" Reintento OK.")
            except Exception as re_e:
                print(f" Reintento fallido: {re_e}"); return False
    print(f"  ✅ Subida a '{worksheet_name}' completada.")
    return True

print("--- [Paso 4/7] Iniciando proceso de extracción... ---")
global_start_time = time.time()
main_process = psutil.Process(os.getpid())
print(f"    (RAM inicial: {main_process.memory_info().rss / 1024**2:.2f} MB)")

counts = {'ok': 0, 'skipped': 0, 'not_found': 0, 'load_error': 0, 'parse_error': 0}
failed_mids = {'not_found': [], 'load': [], 'parse': []}

for range_info in EXTRACTION_RANGES:
    range_start_time = time.time()
    start_id, end_id, label = range_info['start_id'], range_info['end_id'], range_info['label']
    print(f"\n{'='*60}\n--- Procesando Rango: '{label}' (IDs: {start_id} a {end_id}) ---\n{'='*60}")
    
    ids_to_process = list(range(start_id, end_id - 1, -1))
    rows_neg_zero, rows_pos = [], []
    processed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker_task, mid): mid for mid in ids_to_process}
        for future in as_completed(futures):
            processed_count += 1
            mid_completed = futures[future]
            try:
                mid_res, status, result = future.result()
                counts[status] += 1
                if status == 'ok':
                    row_data, ah_num = result
                    if ah_num is not None and ah_num <= 0: rows_neg_zero.append(row_data)
                    else: rows_pos.append(row_data)
                elif status in failed_mids:
                    failed_mids[status].append(result if status != 'not_found' else mid_completed)
                print(f"\r  Progreso '{label}': {processed_count}/{len(ids_to_process)} | OK: {counts['ok']} | Fallos: {counts['load_error'] + counts['parse_error']} | RAM: {main_process.memory_info().rss / 1024**2:.1f}MB", end="")
            except Exception as exc:
                counts['load_error'] += 1; failed_mids['load'].append((mid_completed, str(exc)))
                print(f"\n  [ERROR FATAL] MID {mid_completed}: {exc}")
    
    print(f"\n\n--- Fin Extracción Rango '{label}' ({(time.time() - range_start_time):.2f}s) ---")
    print(f"  Resultados: {len(rows_pos)} para '{NOMBRE_HOJA_POSITIVOS}', {len(rows_neg_zero)} para '{NOMBRE_HOJA_NEG_CERO}'.")

    upload_data_to_sheet(NOMBRE_HOJA_NEG_CERO, rows_neg_zero, COLS, sh)
    upload_data_to_sheet(NOMBRE_HOJA_POSITIVOS, rows_pos, COLS, sh)
    
print("\n" + "="*60)
print("--- [Paso 5/7] Proceso de extracción y subida completado. ---")
print("="*60 + "\n")

print("--- [Paso 6/7] Resumen Final del Proceso ---")
total_duration = time.time() - global_start_time
print(f"⏱️ Tiempo Total de Ejecución: {total_duration / 60:.2f} minutos.")
print(f"✅ Partidos Procesados con Éxito (OK): {counts['ok']}")
print(f"🟡 Partidos Saltados (Sin AH inicial): {counts['skipped']}")
print(f"🔴 Partidos No Encontrados (404): {counts['not_found']}")
print(f"❌ Errores de Carga (Timeout/Driver): {counts['load_error']}")
print(f"❌ Errores de Parseo (HTML inesperado): {counts['parse_error']}")
print(f"🧠 RAM Final: {main_process.memory_info().rss / 1024**2:.2f} MB")
print("\n🎉 ¡Proceso finalizado! Revisa tus hojas de Google Sheets para ver los datos.")