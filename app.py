# app_streamlit.py
import streamlit as st
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import os # Importante para la modificación de Selenium

# Importa la lógica principal del scraper
from modules.estudio_scraper import obtener_datos_completos_partido, format_ah_as_decimal_string_of

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Análisis de Partidos",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- FUNCIÓN PARA LA PÁGINA PRINCIPAL ---
def mostrar_pagina_principal():
    st.title("📈 Próximos Partidos Encontrados")

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

    @st.cache_data(ttl=600)
    async def get_main_page_matches_async():
        async with async_playwright() as p:
            # En la nube, no es necesario especificar el ejecutable si está instalado globalmente
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                await page.goto(URL_NOWGOAL, wait_until="networkidle", timeout=20000)
                await page.wait_for_selector('tr[id^="tr1_"]', timeout=10000)
                html_content = await page.content()
                return parse_main_page_matches(html_content)
            finally:
                await browser.close()
    try:
        with st.spinner("Buscando partidos en Nowgoal... ⚽"):
            all_matches = asyncio.run(get_main_page_matches_async())
        st.sidebar.header("Filtros")
        filter_handicap = st.sidebar.checkbox("Mostrar solo con Hándicap", True)
        filtered_matches = [m for m in all_matches if m.get('handicap') and m.get('handicap') not in ['N/A', '-']] if filter_handicap else all_matches
        st.info(f"Mostrando {len(filtered_matches)} de {len(all_matches)} partidos encontrados.")
        if filtered_matches:
            df = pd.DataFrame(filtered_matches)
            df['Análisis'] = df['id'].apply(lambda id: f"?match_id={id}")
            st.dataframe(df[['time', 'home_team', 'away_team', 'handicap', 'goal_line', 'Análisis']], hide_index=True, use_container_width=True,
                column_config={"Análisis": st.column_config.LinkColumn("Analizar", display_text="📊")})
    except Exception as e:
        st.error(f"No se pudieron cargar los partidos: {e}")

# --- FUNCIÓN PARA LA PÁGINA DE ESTUDIO ---
def mostrar_pagina_estudio(match_id):
    if st.button("⬅️ Volver a la lista de partidos"):
        st.query_params.clear()
        st.rerun()

    @st.cache_data(ttl=3600)
    def obtener_datos_cacheados(m_id):
        return obtener_datos_completos_partido(m_id)

    with st.spinner(f"Realizando análisis completo para el partido ID: {match_id}..."):
        data = obtener_datos_cacheados(match_id)

    if not data or "error" in data:
        st.error(f"Error al obtener datos para el partido {match_id}: {data.get('error', 'Error desconocido.')}")
        return

    st.title("Dashboard de Análisis de Partido")
    st.header(f"{data['home_name']} vs {data['away_name']}")
    st.divider()
    # ... (El resto de la lógica para mostrar los datos del estudio iría aquí, como en la respuesta anterior)
    st.subheader("📊 Clasificación en Liga y Estadísticas O/U")
    # ... etc.

# --- CONTROLADOR PRINCIPAL ---
if 'match_id' in st.query_params:
    mostrar_pagina_estudio(st.query_params['match_id'])
else:
    mostrar_pagina_principal()
