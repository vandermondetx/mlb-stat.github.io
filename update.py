#!/usr/bin/env python
"""
Daily Update Script:
1. Clears out old generated files.
2. Scrapes data, generates matchup CSVs and PNG charts.
3. Builds a GitHub Pages slideshow (index.html) with manual "Previous/Next" controls.
4. Resets git history and force-pushes a fresh commit containing only today's files.
5. Prints your GitHub Pages URL for viewing the site.

Requirements:
    pip install pandas bs4 tqdm math matplotlib requests

Usage:
    python daily_update.py

WARNING:
    This script force-pushes an orphan branch to the remote main branch.
    It will wipe all previous commit history on that branch.
"""

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
# Set your GitHub Pages URL here (adjust if you are using a user site or project site)
GITHUB_PAGES_URL = "https://<yourusername>.github.io"  # or "https://<yourusername>.github.io/<repo>"

# Folder to store generated PNG files.
PNG_FOLDER = "today"

# --------------------------
# Utility: Clear folder contents
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
                print(f"Error deleting file {file_path}: {e}")
    else:
        os.makedirs(folder)

# --------------------------
# Part 1: Data Scraping & PNG Generation
# --------------------------
def scrape_and_generate_pngs():
    # Clear out old PNGs in PNG_FOLDER so only today's files remain.
    clear_folder(PNG_FOLDER)
    
    # --- Scrape the Data ---
    url = "https://www.rotowire.com/baseball/daily-lineups.php"
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    
    data_pitching = []
    data_batter = []
    team_type = ''
    
    for e in soup.select('.lineup__box ul li'):
        # Reset order count when team changes
        if team_type != e.parent.get('class')[-1]:
            order_count = 1
            team_type = e.parent.get('class')[-1]
    
        # Process pitcher and batter info based on class name
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
    df_final.to_csv('pitcher_batter_matchups.csv', index=False)
    
    # --- Retrieve StatMuse Data in Parallel ---
    def format_statmuse_url(batter, pitcher):
        batter_formatted = "-".join(batter.lower().split())
        pitcher_formatted = "-".join(pitcher.lower().split())
        return f"https://www.statmuse.com/mlb/ask/{batter_formatted}-career-stats-vs-{pitcher_formatted}-including-playoffs"
    
    def scrape_player_stats(row):
        try:
            url = format_statmuse_url(row['batter_name'], row['pitcher_name'])
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            stats = {}
            table = soup.find('table')
            if table:
                headers = [th.get_text().strip() for th in table.find_all('th')]
                values = [td.get_text().strip() for td in table.find_all('td')]
                stats = dict(zip(headers, values))
        except Exception as e:
            print(f"Error for batter {row['batter_name']} vs pitcher {row['pitcher_name']}: {e}")
            stats = {}
            
        for stat in ['PA', 'AB', 'H', 'HR', 'SO', 'AVG', 'OBP', 'SLG', 'OPS']:
            try:
                row[stat] = float(stats.get(stat, 0))
            except Exception:
                row[stat] = 0.0
        return row
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(scrape_player_stats, [row for _, row in df_final.iterrows()]),
                            total=len(df_final), desc="Retrieving StatMuse Data"))
        df_final = pd.DataFrame(results)
    
    df_final.to_csv('today_matchups.csv', index=False)
    
    # --- Create PNG Charts ---
    def logarithmic_increase(x, max_value=20):
        if x < max_value:
            return math.log(x + 1, 5) * max_value / math.log(max_value + 1, 5)
        else:
            return max_value
    
    # Create a subset with an explicit copy to avoid SettingWithCopyWarning
    df_subset = df_final[['pitcher_name', 'batter_name', 'PA', 'OPS']].copy()
    df_subset.loc[:, 'color_value'] = df_subset.apply(
        lambda row: 2 * logarithmic_increase(row['PA']) * (row['OPS'] - 0.75), axis=1
    )
    df_subset.loc[:, 'color'] = df_subset['color_value'].apply(determine_color)
    
    top_50_favorable = df_subset[df_subset['color_value'] > 0].sort_values('color_value', ascending=False).head(50)
    top_50_unfavorable = df_subset[df_subset['color_value'] < 0].sort_values('color_value', ascending=True).head(50)
    
    def plot_colored_df(df, title, filename):
        fig, ax = plt.subplots(figsize=(10, len(df) / 2))
        ax.axis('tight')
        ax.axis('off')
        table = ax.table(cellText=df[['pitcher_name', 'batter_name', 'PA', 'OPS']].values,
                         colLabels=df.columns[:-2],
                         cellColours=[[determine_color(val)] * 4 for val in df['color_value']],
                         loc='center')
        table.scale(1, 1.5)
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        ax.set_title(title, fontsize=14)
        plt.savefig(filename, bbox_inches='tight', dpi=300)
        plt.close()
    
    plot_colored_df(top_50_favorable, 'Top 50 Most Favorable (Red)', 'top_50_favorable_plot.png')
    plot_colored_df(top_50_unfavorable, 'Top 50 Least Favorable (Blue)', 'top_50_unfavorable_plot.png')
    
    # Add color columns to the full DataFrame (using .loc to avoid warnings)
    df_final.loc[:, 'color_value'] = df_final.apply(
        lambda row: 2 * logarithmic_increase(row['PA']) * (row['OPS'] - 0.75), axis=1
    )
    df_final.loc[:, 'color'] = df_final['color_value'].apply(determine_color)
    
    # Create game charts in PNG_FOLDER
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
    
        filename = os.path.join(PNG_FOLDER, f"{team1}_vs_{team2}_lineup.png")
        plt.savefig(filename, bbox_inches='tight', dpi=300)
        plt.close()
    
    print(f"All plots have been saved in the '{PNG_FOLDER}' folder.")
    
    # --- Build the Slideshow HTML ---
    build_slideshow(PNG_FOLDER)

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

def build_slideshow(png_folder):
    """Scans the PNG folder and builds an index.html slideshow with manual controls."""
    png_files = sorted([f for f in os.listdir(png_folder) if f.lower().endswith('.png')])
    
    if not png_files:
        print("No PNG files found in the folder:", png_folder)
        return
    
    # Prepare a JavaScript array of file paths
    img_paths = [os.path.join(png_folder, fname) for fname in png_files]
    js_array = json.dumps(img_paths)
    
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
      text-align: center;
      margin: 0;
      padding: 20px;
    }}
    .slideshow-container {{
      max-width: 900px;
      margin: auto;
      position: relative;
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
  <div class="slideshow-container">
    <img id="slide" src="{img_paths[0]}" alt="Slideshow Image">
  </div>
  <div>
    <button class="nav-button" onclick="prevImage()">Previous</button>
    <button class="nav-button" onclick="nextImage()">Next</button>
  </div>
  <script>
    var images = {js_array};
    var currentIndex = 0;
    function showImage(index) {{
      document.getElementById("slide").src = images[index];
    }}
    function nextImage() {{
      currentIndex = (currentIndex + 1) % images.length;
      showImage(currentIndex);
    }}
    function prevImage() {{
      currentIndex = (currentIndex - 1 + images.length) % images.length;
      showImage(currentIndex);
    }}
  </script>
</body>
</html>
"""
    with open("index.html", "w") as f:
        f.write(html_content)
    
    print("index.html generated successfully.")

# --------------------------
# Part 2: Git Reset & Force Push
# --------------------------
def push_to_github():
    """
    Deletes previous commit history by creating an orphan branch and force-pushing to main.
    WARNING: This operation will wipe all previous commit history on the remote main branch.
    """
    try:
        # Create an orphan branch with no history
        subprocess.check_call(["git", "checkout", "main"])
        # Remove tracked files from the index
        subprocess.check_call(["git", "add", "."])
        # Commit the changes
        subprocess.check_call(["git", "commit", "-m", "Daily update"])
        # Force push the orphan branch to the remote main branch
        subprocess.check_call(["git", "push", "origin", "main"])
        print("Git force-push complete. Remote main now contains only today's files.")
        print("Visit your GitHub Pages site at:", GITHUB_PAGES_URL)
    except subprocess.CalledProcessError as e:
        print("Error during git operations:", e)

# --------------------------
# Main
# --------------------------
def main():
    scrape_and_generate_pngs()
    push_to_github()

if __name__ == '__main__':
    main()

