#!/usr/bin/env python
"""
Unified Daily Update Script (ProcessPoolExecutor with module-level scraping):
1. Clears out old generated files.
2. Scrapes data for both today and tomorrow, generating matchup CSVs and PNG charts.
3. Builds a GitHub Pages HTML page (index.html) with four tabs:
     - Today Game Matchups
     - Today Batter-Pitcher Matchups
     - Tomorrow Game Matchups
     - Tomorrow Batter-Pitcher Matchups
4. Resets git history and force-pushes a fresh commit containing only today's files.
5. Prints your GitHub Pages URL for viewing the site.

Requirements:
    pip install pandas bs4 tqdm math matplotlib requests

Usage:
    python unified_daily_update.py

WARNING:
    This script force-pushes an orphan branch to the remote main branch.
    It will wipe all previous commit history on that branch.
"""

# Set the matplotlib backend to "Agg" to avoid Tkinter issues.
import matplotlib
matplotlib.use("Agg")

import os
import math
import json
import subprocess
import shutil
import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from tqdm import tqdm
import matplotlib.pyplot as plt

# --------------------------
# CONFIGURATION
# --------------------------
GITHUB_PAGES_URL = "https://<yourusername>.github.io"  # Adjust for your GitHub Pages URL

# Folders for storing PNG files.
TODAY_GAME_FOLDER = "today_game"
TODAY_BP_FOLDER   = "today_bp"
TOMORROW_GAME_FOLDER = "tomorrow_game"
TOMORROW_BP_FOLDER   = "tomorrow_bp"

# --------------------------
# Utility Functions
# --------------------------
def clear_folder(folder):
    """Delete all files in the specified folder."""
    if os.path.exists(folder):
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
    else:
        os.makedirs(folder)

def determine_color(value):
    """Determines a color tuple based on the value."""
    if value > 0:
        norm_value = value / 100
        red_intensity = min(1, norm_value)
        return (1, 1 - red_intensity, 1 - red_intensity)
    else:
        norm_value = abs(value) / 100
        blue_intensity = min(1, norm_value)
        return (1 - blue_intensity, 1 - blue_intensity, 1)

def logarithmic_increase(x, max_value=20):
    """
    Returns a logarithmically scaled value for PA, capped at max_value.
    This gives diminishing returns as PA increases.
    """
    if x < max_value:
        return math.log(x + 1, 5) * max_value / math.log(max_value + 1, 5)
    else:
        return max_value

def weighted_color_value(pa, ops):
    """
    Improved formula using a logarithmic PA multiplier (base 5).
    
    This ensures that higher PA results in a higher weighted score while 
    still having diminishing marginal returns.
    """
    log_val = logarithmic_increase(pa)
    deviation = ops - 0.75
    if deviation > 0:
      return log_val * min(deviation,0.5) + log_val*0.5
    elif deviation <0:
      return log_val * max(deviation,-0.5) - log_val*0.5
    else:
        return 0

# --------------------------
# Module-Level StatMuse Scraping Functions
# --------------------------
def format_statmuse_url(batter, pitcher):
    batter_formatted = "-".join(batter.lower().split())
    pitcher_formatted = "-".join(pitcher.lower().split())
    return f"https://www.statmuse.com/mlb/ask/{batter_formatted}-career-stats-vs-{pitcher_formatted}-including-playoffs"

def scrape_player_stats(row_dict):
    """
    Accepts a dictionary (row) and retrieves stat data from StatMuse.
    Returns the row dictionary updated with stats.
    """
    row = row_dict.copy()
    try:
        url_sm = format_statmuse_url(row['batter_name'], row['pitcher_name'])
        r = requests.get(url_sm)
        soup_sm = BeautifulSoup(r.content, 'html.parser')
        stats = {}
        table = soup_sm.find('table')
        if table:
            headers = [th.get_text().strip() for th in table.find_all('th')]
            values = [td.get_text().strip() for td in table.find_all('td')]
            stats = dict(zip(headers, values))
    except Exception as ex:
        stats = {}
    for stat in ['PA', 'AB', 'H', 'HR', 'SO', 'AVG', 'OBP', 'SLG', 'OPS']:
        try:
            row[stat] = float(stats.get(stat, 0))
        except Exception:
            row[stat] = 0.0
    return row

# --------------------------
# Scraping & PNG Generation per Day
# --------------------------
def scrape_and_generate_pngs_for(day_label, url, game_folder, bp_folder):
    """
    Scrapes matchup data from the given URL, generates matchup CSVs, batter–pitcher charts (top 50 favorable/unfavorable)
    and game matchup charts, saving them to the given folders.
    The day_label (e.g. "today" or "tomorrow") is appended to filenames.
    """
    # Clear out the target folders so only fresh files remain.
    clear_folder(game_folder)
    clear_folder(bp_folder)
    
    # --- Scrape the Data ---
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    
    data_pitching = []
    data_batter = []
    team_type = ''
    
    for e in soup.select('.lineup__box ul li'):
        # Reset order count when team changes
        if team_type != e.parent.get('class')[-1]:
            order_count = 1
            team_type = e.parent.get('class')[-1]
    
        if e.get('class') and 'lineup__player-highlight' in e.get('class'):
            data_pitching.append({
                'date': e.find_previous('main').get('data-gamedate'),
                'game_time': e.find_previous('div', attrs={'class': 'lineup__time'}).get_text(strip=True),
                'pitcher_name': e.a.get_text(strip=True),
                'team': e.find_previous('div', attrs={'class': team_type}).next.strip(),
                'lineup_throws': e.span.get_text(strip=True)
            })
        elif e.get('class') and 'lineup__player' in e.get('class'):
            data_batter.append({
                'date': e.find_previous('main').get('data-gamedate'),
                'game_time': e.find_previous('div', attrs={'class': 'lineup__time'}).get_text(strip=True),
                'pitcher_name': e.a.get_text(strip=True),
                'team': e.find_previous('div', attrs={'class': team_type}).next.strip(),
                'pos': e.div.get_text(strip=True),
                'batting_order': order_count,
                'lineup_bats': e.span.get_text(strip=True)
            })
            order_count += 1
    
    df_pitching = pd.DataFrame(data_pitching)
    df_batter = pd.DataFrame(data_batter)
    
    # --- Build Matchup Data ---
    matchups = []
    pitcher_index = 0
    
    while pitcher_index < len(df_pitching):
        current_pitcher = df_pitching.iloc[pitcher_index]
        opponent_pitcher = df_pitching.iloc[pitcher_index + 1]
        
        team_batters = df_batter[df_batter['team'] == current_pitcher['team']]
        opponent_batters = df_batter[df_batter['team'] == opponent_pitcher['team']]
    
        for _, batter in team_batters.iterrows():
            matchups.append({
                'date': batter['date'],
                'game_time': batter['game_time'],
                'pitcher_name': opponent_pitcher['pitcher_name'],
                'pitcher_team': opponent_pitcher['team'],
                'pitcher_throws': opponent_pitcher['lineup_throws'],
                'batter_name': batter['pitcher_name'],
                'batter_team': batter['team'],
                'batter_position': batter['pos'],
                'batting_order': batter['batting_order'],
                'batter_bats': batter['lineup_bats']
            })
    
        for _, batter in opponent_batters.iterrows():
            matchups.append({
                'date': batter['date'],
                'game_time': batter['game_time'],
                'pitcher_name': current_pitcher['pitcher_name'],
                'pitcher_team': current_pitcher['team'],
                'pitcher_throws': current_pitcher['lineup_throws'],
                'batter_name': batter['pitcher_name'],
                'batter_team': batter['team'],
                'batter_position': batter['pos'],
                'batting_order': batter['batting_order'],
                'batter_bats': batter['lineup_bats']
            })
    
        pitcher_index += 2
    
    df_final = pd.DataFrame(matchups)
    df_final.to_csv(f'pitcher_batter_matchups_{day_label}.csv', index=False)
    
    # --- Retrieve StatMuse Data using ProcessPoolExecutor ---
    rows = [row.to_dict() for _, row in df_final.iterrows()]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(scrape_player_stats, rows),
                            total=len(rows), desc=f"Retrieving StatMuse Data ({day_label})"))
    df_final = pd.DataFrame(results)
    
    df_final.to_csv(f'matchups_{day_label}.csv', index=False)
    
    # --- Create PNG Charts for Batter-Pitcher Matchups ---
    # Include additional columns: pitcher_team, batter_team, PA, OPS, H, HR, SO
    df_subset = df_final[['pitcher_team', 'pitcher_name', 'batter_team', 'batter_name', 'PA', 'OPS', 'H', 'HR', 'SO']].copy()
    # Calculate new color value using the improved weighted formula with log base 5 multiplier
    df_subset['color_value'] = df_subset.apply(
        lambda row: weighted_color_value(row['PA'], row['OPS']),
        axis=1)
    df_subset['color'] = df_subset['color_value'].apply(determine_color)
    top_50_fav = df_subset[df_subset['color_value'] > 0].sort_values('color_value', ascending=False).head(50)
    top_50_unfav = df_subset[df_subset['color_value'] < 0].sort_values('color_value', ascending=True).head(50)
    
    def plot_colored_df(df, title, filename):
        # Use all columns for display: pitcher_team, pitcher_name, batter_team, batter_name, PA, OPS, H, HR, SO
        display_cols = ['pitcher_team', 'pitcher_name', 'batter_team', 'batter_name', 'PA', 'OPS', 'H', 'HR', 'SO']
        fig, ax = plt.subplots(figsize=(10, len(df) / 2))
        ax.axis('tight')
        ax.axis('off')
        # Build cell colors based on the color_value column
        cell_colors = [[determine_color(val)] * len(display_cols) for val in df['color_value']]
        table = ax.table(cellText=df[display_cols].values,
                         colLabels=display_cols,
                         cellColours=cell_colors,
                         loc='center')
        table.scale(1, 1.5)
        table.auto_set_font_size(False)
        table.set_fontsize(7)
        ax.set_title(title, fontsize=14)
        plt.savefig(filename, bbox_inches='tight', dpi=300)
        plt.close(fig)
    
    plot_colored_df(top_50_fav, f"Top 50 Most Favorable (Red) {day_label}",
                    os.path.join(bp_folder, f"top_50_favorable_{day_label}.png"))
    plot_colored_df(top_50_unfav, f"Top 50 Least Favorable (Blue) {day_label}",
                    os.path.join(bp_folder, f"top_50_unfavorable_{day_label}.png"))
    
    # --- Calculate Color for Full DataFrame using the improved formula ---
    df_final['color_value'] = df_final.apply(
        lambda row: weighted_color_value(row['PA'], row['OPS']),
        axis=1)
    df_final['color'] = df_final['color_value'].apply(determine_color)
    
    # --- Create Game Matchup Charts ---
    unique_games = df_final.groupby(['pitcher_team', 'batter_team']).size().reset_index().drop(0, axis=1)
    game_pairs = [(unique_games.iloc[i, 0], unique_games.iloc[i, 1]) for i in range(0, len(unique_games), 2)]
    
    for team1, team2 in game_pairs:
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 10), constrained_layout=True)
        team1_data = df_final[df_final['batter_team'] == team1].sort_values(by='batting_order')
        team2_data = df_final[df_final['batter_team'] == team2].sort_values(by='batting_order')
        axes[0].barh(team1_data['batter_name'], team1_data['PA'], color=team1_data['color'].tolist())
        axes[0].set_title(f'{team1} Batting Order', fontsize=14)
        axes[0].set_xlabel('Plate Appearances')
        axes[0].set_ylabel('Batter')
        axes[0].invert_yaxis()
        axes[1].barh(team2_data['batter_name'], team2_data['PA'], color=team2_data['color'].tolist())
        axes[1].set_title(f'{team2} Batting Order', fontsize=14)
        axes[1].set_xlabel('Plate Appearances')
        axes[1].invert_yaxis()
    
        filename = os.path.join(game_folder, f"{team1}_vs_{team2}_{day_label}.png")
        plt.savefig(filename, bbox_inches='tight', dpi=300)
        plt.close(fig)
    
    print(f"All plots for {day_label} saved in respective folders.")

# --------------------------
# Build the Combined HTML Slideshow
# --------------------------
def build_slideshow(today_game_folder, today_bp_folder, tomorrow_game_folder, tomorrow_bp_folder):
    """
    Builds index.html with four tabs:
      - Today Game Matchups
      - Today Batter-Pitcher Matchups
      - Tomorrow Game Matchups
      - Tomorrow Batter-Pitcher Matchups
    Each tab displays a manual slideshow of the corresponding PNG images.
    """
    today_game_images = sorted([os.path.join(today_game_folder, f) for f in os.listdir(today_game_folder) if f.lower().endswith('.png')])
    today_bp_images   = sorted([os.path.join(today_bp_folder, f) for f in os.listdir(today_bp_folder) if f.lower().endswith('.png')])
    tomorrow_game_images = sorted([os.path.join(tomorrow_game_folder, f) for f in os.listdir(tomorrow_game_folder) if f.lower().endswith('.png')])
    tomorrow_bp_images   = sorted([os.path.join(tomorrow_bp_folder, f) for f in os.listdir(tomorrow_bp_folder) if f.lower().endswith('.png')])
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLB Matchups</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      background: #f0f0f0;
      margin: 0;
      padding: 20px;
    }}
    .tab {{
      overflow: hidden;
      border-bottom: 1px solid #ccc;
      margin-bottom: 20px;
    }}
    .tab button {{
      background-color: inherit;
      border: none;
      outline: none;
      cursor: pointer;
      padding: 14px 16px;
      transition: 0.3s;
      font-size: 17px;
    }}
    .tab button:hover {{
      background-color: #ddd;
    }}
    .tab button.active {{
      background-color: #ccc;
    }}
    .tabcontent {{
      display: none;
    }}
    .slideshow-container {{
      max-width: 900px;
      margin: auto;
      position: relative;
      text-align: center;
    }}
    img {{
      width: 100%;
      height: auto;
      border: 1px solid #ddd;
      border-radius: 4px;
      padding: 5px;
      background: #fff;
    }}
    .nav-button {{
      font-size: 18px;
      padding: 10px 20px;
      margin: 10px;
      cursor: pointer;
    }}
  </style>
</head>
<body>
  <h1>MLB Matchups</h1>
  <div class="tab">
    <button class="tablinks" onclick="openTab(event, 'TodayGame')" id="defaultOpen">Today Game Matchups</button>
    <button class="tablinks" onclick="openTab(event, 'TodayBP')">Today Batter-Pitcher Matchups</button>
    <button class="tablinks" onclick="openTab(event, 'TomorrowGame')">Tomorrow Game Matchups</button>
    <button class="tablinks" onclick="openTab(event, 'TomorrowBP')">Tomorrow Batter-Pitcher Matchups</button>
  </div>
  
  <div id="TodayGame" class="tabcontent">
    <div class="slideshow-container">
      <img id="todayGameSlide" src="{today_game_images[0] if today_game_images else ''}" alt="Today Game Matchups">
    </div>
    <div>
      <button class="nav-button" onclick="prevTodayGame()">Previous</button>
      <button class="nav-button" onclick="nextTodayGame()">Next</button>
    </div>
  </div>
  
  <div id="TodayBP" class="tabcontent">
    <div class="slideshow-container">
      <img id="todayBPSlide" src="{today_bp_images[0] if today_bp_images else ''}" alt="Today Batter-Pitcher Matchups">
    </div>
    <div>
      <button class="nav-button" onclick="prevTodayBP()">Previous</button>
      <button class="nav-button" onclick="nextTodayBP()">Next</button>
    </div>
  </div>
  
  <div id="TomorrowGame" class="tabcontent">
    <div class="slideshow-container">
      <img id="tomorrowGameSlide" src="{tomorrow_game_images[0] if tomorrow_game_images else ''}" alt="Tomorrow Game Matchups">
    </div>
    <div>
      <button class="nav-button" onclick="prevTomorrowGame()">Previous</button>
      <button class="nav-button" onclick="nextTomorrowGame()">Next</button>
    </div>
  </div>
  
  <div id="TomorrowBP" class="tabcontent">
    <div class="slideshow-container">
      <img id="tomorrowBPSlide" src="{tomorrow_bp_images[0] if tomorrow_bp_images else ''}" alt="Tomorrow Batter-Pitcher Matchups">
    </div>
    <div>
      <button class="nav-button" onclick="prevTomorrowBP()">Previous</button>
      <button class="nav-button" onclick="nextTomorrowBP()">Next</button>
    </div>
  </div>
  
  <script>
    function openTab(evt, tabName) {{
      var i, tabcontent, tablinks;
      tabcontent = document.getElementsByClassName("tabcontent");
      for (i = 0; i < tabcontent.length; i++) {{
        tabcontent[i].style.display = "none";
      }}
      tablinks = document.getElementsByClassName("tablinks");
      for (i = 0; i < tablinks.length; i++) {{
        tablinks[i].className = tablinks[i].className.replace(" active", "");
      }}
      document.getElementById(tabName).style.display = "block";
      evt.currentTarget.className += " active";
    }}
    document.getElementById("defaultOpen").click();
    
    var todayGameImages = {json.dumps(today_game_images)};
    var todayGameIndex = 0;
    function showTodayGameImage(index) {{
      document.getElementById("todayGameSlide").src = todayGameImages[index];
    }}
    function nextTodayGame() {{
      todayGameIndex = (todayGameIndex + 1) % todayGameImages.length;
      showTodayGameImage(todayGameIndex);
    }}
    function prevTodayGame() {{
      todayGameIndex = (todayGameIndex - 1 + todayGameImages.length) % todayGameImages.length;
      showTodayGameImage(todayGameIndex);
    }}
    
    var todayBPImages = {json.dumps(today_bp_images)};
    var todayBPIndex = 0;
    function showTodayBPImage(index) {{
      document.getElementById("todayBPSlide").src = todayBPImages[index];
    }}
    function nextTodayBP() {{
      todayBPIndex = (todayBPIndex + 1) % todayBPImages.length;
      showTodayBPImage(todayBPIndex);
    }}
    function prevTodayBP() {{
      todayBPIndex = (todayBPIndex - 1 + todayBPImages.length) % todayBPImages.length;
      showTodayBPImage(todayBPIndex);
    }}
    
    var tomorrowGameImages = {json.dumps(tomorrow_game_images)};
    var tomorrowGameIndex = 0;
    function showTomorrowGameImage(index) {{
      document.getElementById("tomorrowGameSlide").src = tomorrowGameImages[index];
    }}
    function nextTomorrowGame() {{
      tomorrowGameIndex = (tomorrowGameIndex + 1) % tomorrowGameImages.length;
      showTomorrowGameImage(tomorrowGameIndex);
    }}
    function prevTomorrowGame() {{
      tomorrowGameIndex = (tomorrowGameIndex - 1 + tomorrowGameImages.length) % tomorrowGameImages.length;
      showTomorrowGameImage(tomorrowGameIndex);
    }}
    
    var tomorrowBPImages = {json.dumps(tomorrow_bp_images)};
    var tomorrowBPIndex = 0;
    function showTomorrowBPImage(index) {{
      document.getElementById("tomorrowBPSlide").src = tomorrowBPImages[index];
    }}
    function nextTomorrowBP() {{
      tomorrowBPIndex = (tomorrowBPIndex + 1) % tomorrowBPImages.length;
      showTomorrowBPImage(tomorrowBPIndex);
    }}
    function prevTomorrowBP() {{
      tomorrowBPIndex = (tomorrowBPIndex - 1 + tomorrowBPImages.length) % tomorrowBPImages.length;
      showTomorrowBPImage(tomorrowBPIndex);
    }}
  </script>
</body>
</html>
"""
    with open("index.html", "w") as f:
        f.write(html_content)
    
    print("index.html generated successfully.")

# --------------------------
# Git Operations
# --------------------------
def push_to_github():
    """
    Deletes previous commit history by committing all changes to main and pushing to remote.
    WARNING: This operation will wipe all previous commit history on the remote main branch.
    """
    try:
        subprocess.check_call(["git", "checkout", "main"])
        subprocess.check_call(["git", "add", "."])
        subprocess.check_call(["git", "commit", "-m", "Daily update"])
        subprocess.check_call(["git", "push", "origin", "main"])
        print("Git push complete. Remote main now contains only today's files.")
        print("Visit your GitHub Pages site at:", GITHUB_PAGES_URL)
    except subprocess.CalledProcessError as e:
        print("Error during git operations:", e)

# --------------------------
# Main
# --------------------------
def main():
    # Scrape and generate files for today
    today_url = "https://www.rotowire.com/baseball/daily-lineups.php"
    scrape_and_generate_pngs_for("today", today_url, TODAY_GAME_FOLDER, TODAY_BP_FOLDER)
    
    # Scrape and generate files for tomorrow
    tomorrow_url = "https://www.rotowire.com/baseball/daily-lineups.php?date=tomorrow"
    scrape_and_generate_pngs_for("tomorrow", tomorrow_url, TOMORROW_GAME_FOLDER, TOMORROW_BP_FOLDER)
    
    # Build the HTML slideshow with four tabs
    build_slideshow(TODAY_GAME_FOLDER, TODAY_BP_FOLDER, TOMORROW_GAME_FOLDER, TOMORROW_BP_FOLDER)
    
    # Push changes to GitHub
    push_to_github()

if __name__ == '__main__':
    main()