import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import os
from logger_config import get_logger

logger = get_logger("scraper")

LINKS = [
    "/en/squads/18bb7c10/Arsenal-Stats",
    "/en/squads/8602292d/Aston-Villa-Stats",
    "/en/squads/4ba7cbea/Bournemouth-Stats",
    "/en/squads/cd051869/Brentford-Stats",
    "/en/squads/d07537b9/Brighton-and-Hove-Albion-Stats",
    "/en/squads/cff3d9bb/Chelsea-Stats",
    "/en/squads/47c64c55/Crystal-Palace-Stats",
    "/en/squads/d3fd31cc/Everton-Stats",
    "/en/squads/fd962109/Fulham-Stats",
    "/en/squads/b74092de/Ipswich-Town-Stats",
    "/en/squads/a2d435b3/Leicester-City-Stats",
    "/en/squads/822bd0ba/Liverpool-Stats",
    "/en/squads/b8fd03ef/Manchester-City-Stats",
    "/en/squads/19538871/Manchester-United-Stats",
    "/en/squads/b2b47a98/Newcastle-United-Stats",
    "/en/squads/e4a775cb/Nottingham-Forest-Stats",
    "/en/squads/33c895d4/Southampton-Stats",
    "/en/squads/361ca564/Tottenham-Hotspur-Stats",
    "/en/squads/7c21e445/West-Ham-United-Stats",
    "/en/squads/8cec06e1/Wolverhampton-Wanderers-Stats",
]
TEAMS = [
    "Arsenal",
    "Aston Villa",
    "Bournemouth",
    "Brentford",
    "Brighton",
    "Chelsea",
    "Crystal Palace",
    "Everton",
    "Fulham",
    "Ipswich Town",
    "Leicester City",
    "Liverpool",
    "Manchester City",
    "Manchester Utd",
    "Newcastle Utd",
    "Nott'ham Forest",
    "Southampton",
    "Tottenham",
    "West Ham",
    "Wolves",
]
TABLE_NAMES = (
    "all_stats_standard",
    "all_stats_keeper",
    "all_stats_keeper_adv",
    "all_stats_shooting",
    "all_stats_passing",
    "all_stats_passing_types",
    "all_stats_gca",
    "all_stats_defense",
    "all_stats_possession",
    "all_stats_playing_time",
    "all_stats_misc",
)


class RateLimitException(Exception):
    pass


def _get_all_current_teams() -> pd.DataFrame:
    URL = (
        f"https://{os.environ["WEBSITE_NAME"]}."
        "com/en/comps/9/Premier-League-Stats"
    )
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    try:
        find_squad_stats = soup.find(id="stats_squads_standard_for")
        teams = find_squad_stats.findAll("a")
    except Exception:
        raise RateLimitException("You got rate limited.")
    team_names = [each.text for each in teams]
    team_links = [
        str(each).split("href")[1].split('="')[1].split('"')[0]
        for each in teams
    ]
    teams_df = pd.DataFrame(
        list(zip(team_names, team_links)), columns=["teams", "links"]
    )
    return teams_df


def _scrape_stat_table(
    soup: BeautifulSoup, table_id: str
) -> pd.DataFrame:
    try:
        find_table = soup.find(id=table_id)
        headers = [str(each) for each in find_table.findAll("td")]
    except Exception:
        raise RateLimitException("You got rate limited.")
    headers = [
        each.split("data-stat")[1].split('="')[1].split('"')[0]
        for each in headers
    ]
    seen = set()
    seen_add = seen.add
    headers = [x for x in headers if not (x in seen or seen_add(x))]
    actual_stats = [each.text for each in find_table.findAll("td")]
    stat = pd.DataFrame(
        np.array(actual_stats).reshape(
            int(len(actual_stats) / len(headers)), len(headers)
        ),
        columns=headers,
    )
    players = find_table.findAll("th", class_="left")
    players = [each.text for each in players][1:]
    stat["player"] = players
    return stat


def run() -> pd.DataFrame:
    table_dic = {name: [] for name in TABLE_NAMES}
    for team, team_link in zip(TEAMS, LINKS):
        logger.info(f"Team: {team}")
        # To avoid rate-limiting and getting banned.
        time.sleep(8)
        URL = f"https://{os.environ["WEBSITE_NAME"]}.com{team_link}"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        for each in table_dic.keys():
            df = _scrape_stat_table(soup, each)
            df["team"] = team
            if "matches" in df:
                df.drop("matches", axis=1, inplace=True)
            table_dic[each].append(df)

    for key in table_dic.keys():
        # Concat the lists together and set it to the key-value
        table_dic[key] = pd.concat(table_dic[key])

    merged = table_dic["all_stats_standard"]
    for key in list(table_dic.keys())[1:]:
        # Drop columns that are duplicates after merging first.
        merged = merged.drop(
            [
                x
                for x in merged.columns
                if (x.endswith("_x") or x.endswith("_y"))
            ],
            axis=1,
        )
        merged = merged.merge(
            table_dic[key],
            on=["player", "nationality", "team"],
            how="outer",
            suffixes=("", "_y"),
        )

    merged.fillna(0, inplace=True)
    merged.replace(" ", 0, inplace=True)
    merged.replace("", 0, inplace=True)
    merged = merged.loc[:, ~merged.columns.duplicated()]
    merged = merged.drop_duplicates()
    merged.set_index("player", inplace=True)
    logger.info("Finished merging")
    return merged


if __name__ == "__main__":
    df = run()
    print(df)
