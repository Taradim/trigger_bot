#!/usr/bin/env python3
"""
Script ticker_list.py pour créer une liste de tickers à importer dans TradingView
"""

import pandas as pd
import os
import glob
from datetime import datetime


def get_latest_top_monde_file():
    """Récupère le fichier TOP MONDE avec la date la plus récente dans ticker_room"""

    ticker_room_path = os.path.join("data", "ticker_room")
    top_monde_files = glob.glob(os.path.join(ticker_room_path, "TOP MONDE*.csv"))

    if not top_monde_files:
        raise FileNotFoundError("Aucun fichier TOP MONDE trouvé dans data/ticker_room/")

    # Trouver le fichier avec la date la plus récente
    latest_file = None
    latest_date = None

    for file_path in top_monde_files:
        # Extraire la date du nom de fichier (format: TOP MONDE_YYYY-MM-DD.csv)
        filename = os.path.basename(file_path)
        try:
            # Gérer les différents formats de nom de fichier
            if "TOP MONDE_" in filename:
                date_str = filename.replace("TOP MONDE_", "").replace(".csv", "")
                # Nettoyer la date (enlever les caractères supplémentaires)
                date_str = date_str.split(" ")[0]  # Prendre la première partie
                file_date = datetime.strptime(date_str, "%Y-%m-%d")

                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file_path
        except ValueError:
            continue

    if latest_file is None:
        raise ValueError("Aucun fichier TOP MONDE avec une date valide trouvé")

    print(
        f"📁 Utilisation du fichier TOP MONDE le plus récent : {os.path.basename(latest_file)}"
    )
    return latest_file


def create_ticker_lists():
    """Crée les listes de tickers au format TradingView"""

    try:
        # Récupérer le fichier le plus récent
        latest_file = get_latest_top_monde_file()

        # Lire le fichier CSV
        df = pd.read_csv(latest_file)
        print(f"📊 Données chargées : {df.shape[0]} lignes × {df.shape[1]} colonnes")

        # Vérifier que les colonnes nécessaires existent
        required_columns = ["Symbole", "Marché", "Capitalisation boursière", "score"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Colonnes manquantes : {missing_columns}")

        # Convertir la capitalisation en numérique et nettoyer
        df["Capitalisation boursière"] = pd.to_numeric(
            df["Capitalisation boursière"], errors="coerce"
        )

        # Filtrer les sociétés de plus de 10 milliards (10,000,000,000)
        df_filtered = df[df["Capitalisation boursière"] >= 10000000000].copy()
        print(f"🏢 Sociétés de plus de 10 milliards : {len(df_filtered)}")

        # Trier par score décroissant
        df_filtered = df_filtered.sort_values(by="score", ascending=False)

        # Créer la liste des top 30 par score (sociétés > 10 milliards)
        top_30 = df_filtered.head(30)

        # Créer la liste des top 50 global (toutes sociétés, triées par score)
        df_all_sorted = df.sort_values(by="score", ascending=False)
        top_50_global = df_all_sorted.head(50)

        # Générer les listes au format marché:symbole
        def format_ticker_list(dataframe, title):
            ticker_list = []
            for _, row in dataframe.iterrows():
                market = row["Marché"]
                symbol = row["Symbole"]
                ticker_list.append(f"{market}:{symbol}")
            return ticker_list

        top_30_list = format_ticker_list(top_30, "Top 30 par score (>10Mds)")
        top_50_global_list = format_ticker_list(top_50_global, "Top 50 global")

        # Créer une liste unique avec sections et sans doublons
        def create_unified_list(top_30_list, top_50_global_list):
            unified_list = []

            # Section 1: Top 30 Big
            unified_list.append("// Top 30 Big")
            unified_list.extend(top_30_list)

            # Section 2: Top 50 Global (en excluant les doublons de la section 1)
            unified_list.append("// Top 50 Global")

            # Créer un set des tickers déjà présents dans la section 1
            top_30_set = set(top_30_list)

            # Ajouter les tickers de la section 2 en excluant les doublons
            for ticker in top_50_global_list:
                if ticker not in top_30_set:
                    unified_list.append(ticker)

            return unified_list

        unified_ticker_list = create_unified_list(top_30_list, top_50_global_list)

        # Créer la liste des tickers avec score >= 2.7
        df_score_filtered = df[df["score"] >= 2.7].copy()
        df_score_filtered = df_score_filtered.sort_values(by="score", ascending=False)

        # Générer la liste des tickers score >= 2.7
        score_27_list = format_ticker_list(df_score_filtered, "Score >= 2.7")

        # Sauvegarder les listes

        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)

        # 1. Liste unifiée avec la date actuelle
        today_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"top_monde_{today_date}.txt"
        unified_path = os.path.join(output_dir, filename)

        with open(unified_path, "w", encoding="utf-8") as f:
            for ticker in unified_ticker_list:
                f.write(f"{ticker}\n")

        # 2. Liste des tickers avec score >= 2.7
        score_27_filename = f"top_monde_2_7_{today_date}.txt"
        score_27_path = os.path.join(output_dir, score_27_filename)
        with open(score_27_path, "w", encoding="utf-8") as f:
            for ticker in score_27_list:
                f.write(f"{ticker}\n")

        print(f"\n✅ Listes de tickers générées avec succès !")
        print(f"📄 Liste unifiée : {unified_path}")
        print(f"📄 Score >= 2.7 : {score_27_path}")

        # Afficher un résumé
        print(f"\n📊 Résumé des listes générées :")
        print(f"Section 'Top 30 Big' : {len(top_30_list)} tickers")

        # Compter les tickers uniques dans la section 2
        top_30_set = set(top_30_list)
        unique_global_tickers = [t for t in top_50_global_list if t not in top_30_set]
        print(
            f"Section 'Top 50 Global' (sans doublons) : {len(unique_global_tickers)} tickers"
        )
        print(
            f"Total unique (liste unifiée) : {len(unified_ticker_list) - 2} tickers"
        )  # -2 pour les lignes de section
        print(f"Score >= 2.7 : {len(score_27_list)} tickers")

        # Afficher les premiers tickers de chaque section
        print(f"\n🏆 Top 5 de la section 'Top 30 Big' :")
        for i, ticker in enumerate(top_30_list[:5], 1):
            print(f"  {i}. {ticker}")

        print(f"\n🌍 Top 5 de la section 'Top 50 Global' (sans doublons) :")
        for i, ticker in enumerate(unique_global_tickers[:5], 1):
            print(f"  {i}. {ticker}")

        return True

    except Exception as e:
        print(f"❌ Erreur lors de la création des listes : {e}")
        return False


if __name__ == "__main__":
    print("🚀 Démarrage de ticker_list.py...")
    print("📋 Création des listes de tickers pour TradingView...")

    success = create_ticker_lists()

    if success:
        print("\n✅ Génération des listes terminée avec succès !")
        print(
            "💡 Vous pouvez maintenant copier le contenu des fichiers .txt dans TradingView"
        )
    else:
        print("\n❌ Échec de la génération des listes.")
