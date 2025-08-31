import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import time
import warnings
import glob

# Supprimer tous les warnings pour une sortie plus propre
warnings.filterwarnings("ignore")


def get_top_monde_tickers():
    """Récupère les tickers depuis le fichier TOP MONDE le plus récent"""

    # Chercher tous les fichiers TOP MONDE dans le dossier history
    history_path = os.path.join("data", "history")
    top_monde_files = glob.glob(os.path.join(history_path, "TOP MONDE_*.csv"))

    if not top_monde_files:
        raise FileNotFoundError("Aucun fichier TOP MONDE trouvé dans data/history/")

    # Trouver le fichier avec la date la plus récente
    latest_file = None
    latest_date = None

    for file_path in top_monde_files:
        # Extraire la date du nom de fichier (format: TOP MONDE_YYYY-MM-DD.csv)
        filename = os.path.basename(file_path)
        try:
            date_str = filename.replace("TOP MONDE_", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d")

            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file = file_path
        except ValueError:
            continue

    if latest_file is None:
        raise ValueError("Aucun fichier TOP MONDE avec une date valide trouvé")

    print(
        f"Utilisation du fichier TOP MONDE le plus récent : {os.path.basename(latest_file)}"
    )

    # Lire le fichier CSV et extraire la colonne Symbole
    try:
        df = pd.read_csv(latest_file, sep=",")
        # La première colonne devrait être "Symbole"
        symbol_column = df.columns[0]
        if symbol_column != "Symbole":
            print(
                f"Attention: La première colonne est '{symbol_column}', pas 'Symbole'"
            )

        tickers = df[symbol_column].dropna().tolist()
        print(f"Nombre de tickers chargés depuis TOP MONDE : {len(tickers)}")

        return tickers

    except Exception as e:
        print(f"Erreur lors de la lecture du fichier TOP MONDE : {e}")
        raise


def get_sp500_current():
    # Récupérer les données Wikipedia
    tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    current_df = tables[0]

    # Extraire les tickers actuels
    current_tickers = current_df["Symbol"].str.replace(".", "-", regex=False).tolist()

    print(f"Composition actuelle du S&P 500: {len(current_tickers)} entreprises")
    return current_tickers


def get_monthly_data(tickers, months=13):
    # Calculer les dates de début et fin (13 derniers mois)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)  # Approximation pour 13 mois

    print(
        f"Récupération des données pour {len(tickers)} tickers du {start_date.strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}"
    )

    all_data = []

    # Traiter par lots de 100 pour éviter les limitations
    for i in range(0, len(tickers), 100):
        batch = tickers[i : i + 100]
        print(
            f"Traitement du lot {i//100 + 1}/{(len(tickers)-1)//100 + 1} ({len(batch)} tickers)"
        )

        try:
            # Télécharger les données mensuelles
            data = yf.download(
                batch,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1mo",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
            )

            # Traiter chaque ticker du lot
            for ticker in batch:
                # Récupérer les données pour ce ticker spécifique
                if len(batch) == 1:
                    ticker_data = data.copy()
                else:
                    if (ticker,) in data.columns:
                        ticker_data = data[ticker].copy()
                    elif ticker in data.columns.get_level_values(0):
                        ticker_data = data[ticker].copy()
                    else:
                        continue

                # Récupérer les infos fondamentales
                try:
                    stock = yf.Ticker(ticker)
                    info = stock.info
                    name = info.get("longName", ticker)
                    mcap_now = info.get("marketCap", None)
                    price_now = info.get("regularMarketPrice", None)

                    if mcap_now and price_now and price_now > 0:
                        shares = mcap_now / price_now
                    else:
                        shares = None

                except Exception:
                    name = ticker
                    shares = None

                # Traiter les données mensuelles
                ticker_data = ticker_data.reset_index()
                for _, row in ticker_data.iterrows():
                    open_price = row.get("Open")
                    close_price = row.get("Close")
                    date_val = row.get("Date")

                    if open_price is None or close_price is None or date_val is None:
                        continue

                    # Calculer le market cap estimé
                    mcap_est = (
                        shares * open_price
                        if shares and open_price and shares > 0
                        else None
                    )

                    all_data.append(
                        {
                            "ticker": ticker,
                            "date": date_val,
                            "open": open_price,
                            "close": close_price,
                            "name": name,
                            "market_cap_est": mcap_est,
                        }
                    )

                time.sleep(0.1)  # Éviter le rate limit

        except Exception as e:
            print(f"Erreur lors du traitement du lot {batch}: {e}")
            continue

    return pd.DataFrame(all_data)


def calculate_performance_metrics(monthly_data):
    """Calcule les performances sur toutes les périodes de 12 mois disponibles"""
    print("Calcul des métriques de performance...")

    performance_data = []

    # Grouper par ticker pour traiter chaque entreprise
    for ticker in monthly_data["ticker"].unique():
        ticker_data = monthly_data[monthly_data["ticker"] == ticker].copy()
        ticker_data = ticker_data.sort_values("date")

        if len(ticker_data) < 12:  # Besoin d'au moins 12 mois de données
            continue

        # Récupérer les données des derniers mois
        latest_date = ticker_data["date"].max()
        earliest_date = ticker_data["date"].min()

        # Calculer combien de périodes de 12 mois nous pouvons avoir
        total_months = len(ticker_data)
        available_periods = (
            total_months - 11
        )  # Pour avoir des périodes complètes de 12 mois

        ticker_performances = []

        # Calculer les performances pour chaque période de 12 mois disponible
        for period in range(available_periods):
            # Date de fin de la période (12 mois après le début)
            period_end_date = earliest_date + pd.DateOffset(months=11 + period)

            # Vérifier que la période est dans nos données
            if period_end_date > latest_date:
                break

            # Trouver les dates de référence pour cette période
            date_12m = earliest_date + pd.DateOffset(months=period)
            date_6m = earliest_date + pd.DateOffset(months=6 + period)
            date_3m = earliest_date + pd.DateOffset(months=9 + period)

            # Récupérer les prix de référence
            price_12m = (
                ticker_data[ticker_data["date"] >= date_12m]["close"].iloc[0]
                if len(ticker_data[ticker_data["date"] >= date_12m]) > 0
                else None
            )
            price_6m = (
                ticker_data[ticker_data["date"] >= date_6m]["close"].iloc[0]
                if len(ticker_data[ticker_data["date"] >= date_6m]) > 0
                else None
            )
            price_3m = (
                ticker_data[ticker_data["date"] >= date_3m]["close"].iloc[0]
                if len(ticker_data[ticker_data["date"] >= date_3m]) > 0
                else None
            )
            price_current = (
                ticker_data[ticker_data["date"] >= period_end_date]["close"].iloc[-1]
                if len(ticker_data[ticker_data["date"] >= period_end_date]) > 0
                else None
            )

            # Vérifier que tous les prix sont valides (pas None, pas NaT, et > 0)
            if (
                pd.notna(price_12m)
                and pd.notna(price_6m)
                and pd.notna(price_3m)
                and pd.notna(price_current)
                and price_12m > 0
                and price_6m > 0
                and price_3m > 0
                and price_current > 0
            ):
                # Calculer les performances
                perf_12m = (price_current - price_12m) / price_12m * 100
                perf_6m = (price_current - price_6m) / price_6m * 100
                perf_3m = (price_current - price_3m) / price_3m * 100

                # Calculer le score total
                score_total = perf_12m + perf_6m + perf_3m

                ticker_performances.append(
                    {
                        "period_start": date_12m,
                        "period_end": period_end_date,
                        "perf_12m_pct": perf_12m,
                        "perf_6m_pct": perf_6m,
                        "perf_3m_pct": perf_3m,
                        "score_total": score_total,
                    }
                )

        # Récupérer le nom de l'entreprise
        company_name = ticker_data["name"].iloc[0]

        # Ajouter toutes les périodes pour ce ticker
        for perf in ticker_performances:
            performance_data.append(
                {
                    "ticker": ticker,
                    "name": company_name,
                    "period_start": perf["period_start"],
                    "period_end": perf["period_end"],
                    "perf_12m_pct": perf["perf_12m_pct"],
                    "perf_6m_pct": perf["perf_6m_pct"],
                    "perf_3m_pct": perf["perf_3m_pct"],
                    "score_total": perf["score_total"],
                }
            )

    return pd.DataFrame(performance_data)


def check_existing_data():
    """Vérifie si des données existent déjà et sont à jour en se basant sur la date de modification du fichier"""
    csv_path = os.path.join("data", "history", "top_monde_monthly_data.csv")

    if not os.path.exists(csv_path):
        return None, False

    try:
        # Vérifier la date de modification du fichier CSV
        file_mod_time = os.path.getmtime(csv_path)
        file_mod_date = datetime.fromtimestamp(file_mod_time).date()
        today = datetime.now().date()

        # Les données sont considérées à jour si le fichier a été modifié aujourd'hui
        is_up_to_date = file_mod_date == today

        print(f"Fichier CSV trouvé : {csv_path}")
        print(f"Date de dernière modification : {file_mod_date}")
        print(f"Date actuelle : {today}")
        print(f"Données à jour : {'Oui' if is_up_to_date else 'Non'}")

        # Lire le fichier pour retourner les données
        existing_data = pd.read_csv(csv_path)
        existing_data["date"] = pd.to_datetime(existing_data["date"])

        return existing_data, is_up_to_date

    except Exception as e:
        print(f"Erreur lors de la lecture du CSV existant : {e}")
        return None, False


# Vérifier s'il existe déjà des données
existing_data, is_up_to_date = check_existing_data()

if existing_data is not None and is_up_to_date:
    print("\nUtilisation des données existantes du CSV (déjà à jour)")
    monthly_data = existing_data
else:
    print("\nTéléchargement de nouvelles données...")
    # Récupérer les tickers depuis le fichier TOP MONDE le plus récent
    current_tickers = get_top_monde_tickers()

    # Récupérer les données des 13 derniers mois
    monthly_data = get_monthly_data(current_tickers, months=13)

    # Sauvegarder les données mensuelles dans un CSV
    output_dir = os.path.join("data", "history")
    os.makedirs(output_dir, exist_ok=True)
    monthly_output_path = os.path.join(output_dir, "top_monde_monthly_data.csv")
    monthly_data.to_csv(monthly_output_path, index=False)
    print(f"Sauvegardé: {len(monthly_data)} lignes dans {monthly_output_path}")

print(f"\nDonnées récupérées : {len(monthly_data)} lignes")
print(
    f"Période couverte : du {monthly_data['date'].min()} au {monthly_data['date'].max()}"
)
print(f"Nombre de tickers avec données : {monthly_data['ticker'].nunique()}")

# Calculer les métriques de performance
performance_df = calculate_performance_metrics(monthly_data)

print(
    f"\nMétriques de performance calculées pour {len(performance_df)} entrées (toutes périodes confondues)"
)

# Afficher les périodes disponibles
print(f"\nPériodes de 12 mois analysées :")
periods = (
    performance_df[["period_start", "period_end"]]
    .drop_duplicates()
    .sort_values("period_start")
)
for i, (_, row) in enumerate(periods.iterrows()):
    print(
        f"Période {i+1}: {row['period_start'].strftime('%Y-%m')} à {row['period_end'].strftime('%Y-%m')}"
    )

# Afficher le top 10 des tickers avec le plus haut score sur la période la plus récente
latest_period = performance_df["period_end"].max()
latest_performance = performance_df[performance_df["period_end"] == latest_period]

print(
    f"\nTop 10 des tickers TOP MONDE avec le plus haut score total sur la période la plus récente ({latest_period.strftime('%Y-%m')}) :"
)
top_score = latest_performance.nlargest(10, "score_total")[
    ["ticker", "name", "perf_12m_pct", "perf_6m_pct", "perf_3m_pct", "score_total"]
]
print(top_score.round(2))

# Afficher aussi le top 10 global sur toutes les périodes
print(
    f"\nTop 10 des tickers TOP MONDE avec le plus haut score total sur toutes les périodes :"
)
top_score_global = performance_df.nlargest(10, "score_total")[
    ["ticker", "name", "period_start", "period_end", "score_total"]
]
print(top_score_global.round(2))

# Sauvegarder les résultats des meilleurs performers de toutes les périodes disponibles
print(
    f"\nSauvegarde des résultats des meilleurs performers pour toutes les périodes..."
)

# Définir output_dir au cas où on utilise les données existantes
output_dir = os.path.join("data", "history")

# Créer un DataFrame avec les top 10 de chaque période
all_top_performers = []

# Récupérer toutes les périodes disponibles, triées par date de fin (plus récente en premier)
all_periods = (
    performance_df[["period_start", "period_end"]]
    .drop_duplicates()
    .sort_values("period_end", ascending=False)
)

for i, (_, period_row) in enumerate(all_periods.iterrows()):
    period_start = period_row["period_start"]
    period_end = period_row["period_end"]

    # Filtrer les données pour cette période spécifique
    period_data = performance_df[
        (performance_df["period_start"] == period_start)
        & (performance_df["period_end"] == period_end)
    ]

    # Récupérer le top 10 de cette période
    top_10_period = period_data.nlargest(10, "score_total")[
        ["ticker", "name", "perf_12m_pct", "perf_6m_pct", "perf_3m_pct", "score_total"]
    ].copy()

    # Ajouter les informations de période
    top_10_period["period_start"] = period_start
    top_10_period["period_end"] = period_end
    top_10_period["period_rank"] = range(1, 11)  # Classement 1 à 10

    # Réorganiser les colonnes
    top_10_period = top_10_period[
        [
            "period_start",
            "period_end",
            "period_rank",
            "ticker",
            "name",
            "perf_12m_pct",
            "perf_6m_pct",
            "perf_3m_pct",
            "score_total",
        ]
    ]

    all_top_performers.append(top_10_period)

# Combiner tous les résultats
if all_top_performers:
    combined_top_performers = pd.concat(all_top_performers, ignore_index=True)

    # Sauvegarder dans un CSV
    top_performers_output_path = os.path.join(
        output_dir, "top_monde_performers_all_periods.csv"
    )
    combined_top_performers.to_csv(top_performers_output_path, index=False)
    print(
        f"Sauvegardé: Top 10 de toutes les périodes dans {top_performers_output_path}"
    )
    print(
        f"Total: {len(combined_top_performers)} entrées pour {len(all_periods)} périodes"
    )

    # Afficher un résumé des périodes sauvegardées
    print(f"\nPériodes sauvegardées dans le fichier:")
    for i, (_, period_row) in enumerate(all_periods.iterrows()):
        print(
            f"Période {i+1}: {period_row['period_start'].strftime('%Y-%m')} à {period_row['period_end'].strftime('%Y-%m')} (Top 10)"
        )
else:
    print("Aucune donnée de performance disponible pour la sauvegarde")

# Garder les DataFrames en mémoire (pas de sauvegarde CSV)
print(f"\nDataFrames disponibles :")
print(f"- 'monthly_data' : {len(monthly_data)} lignes de données mensuelles")
print(
    f"- 'performance_df' : {len(performance_df)} lignes de métriques de performance (toutes périodes)"
)
print(f"- 'top_score' : Top 10 des meilleurs performers de la période la plus récente")
