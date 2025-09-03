import asyncio
import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

URL = "https://live20.nowgoal25.com/"

def parse_match_data_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))

    upcoming_matches = []
    now_utc = datetime.datetime.utcnow()

    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id:
            continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'):
            continue
        
        try:
            match_time_str = time_cell['data-t']
            match_time = datetime.datetime.strptime(match_time_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, IndexError):
            continue

        if match_time < now_utc:
            continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        home_team_name = home_team_tag.text.strip() if home_team_tag else "N/A"
        away_team_name = away_team_tag.text.strip() if away_team_tag else "N/A"

        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        upcoming_matches.append({
            "id": match_id,
            "time": match_time.strftime('%Y-%m-%d %H:%M'),
            "home_team": home_team_name,
            "away_team": away_team_name,
            "handicap": handicap,
            "goal_line": goal_line
        })

    upcoming_matches.sort(key=lambda x: x['time'])
    return upcoming_matches[:20]

async def main():
    print("Iniciando el scraper...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print(f"Navegando a {URL}...")
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            
            # Espera explícita para asegurar que los scripts de la página se ejecutan
            print("Página cargada. Esperando a que los datos de los partidos se carguen...")
            await page.wait_for_timeout(5000) 

            html_content = await page.content()
            next_20_matches = parse_match_data_from_html(html_content)

            print(f"Se encontraron {len(next_20_matches)} partidos próximos.")
            print("\n--- PRÓXIMOS 20 PARTIDOS ENCONTRADOS ---\n")
            if not next_20_matches:
                print("No se encontraron próximos partidos.")
            else:
                for match in next_20_matches:
                    print(f"ID: {match['id']}, Hora: {match['time']}, {match['home_team']} vs {match['away_team']}, Handicap: {match['handicap']}, Goles: {match['goal_line']}")
            
            print("\n--- FIN DE LA EXTRACCIÓN ---")

        except Exception as e:
            print(f"Ocurrió un error: {e}")
        finally:
            await browser.close()
            print("Navegador cerrado. Fin del script.")

if __name__ == "__main__":
    asyncio.run(main())
