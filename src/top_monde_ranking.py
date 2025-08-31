#!/usr/bin/env python3
"""
Script top_monde_ranking pour analyser et enrichir les fichiers CSV TOP MONDE
"""

import pandas as pd
import os
import numpy as np


def top_monde_ranking():
    """Analyser et enrichir les fichiers CSV TOP MONDE avec de nouvelles colonnes calculées"""

    # Chemin des dossiers
    waiting_room_path = os.path.join("data", "waiting_room")
    ready_to_use_path = os.path.join("data", "ready_to_use")

    # Créer le dossier ready_to_use s'il n'existe pas
    os.makedirs(ready_to_use_path, exist_ok=True)

    # Chercher tous les fichiers TOP MONDE dans waiting_room
    import glob

    pattern = os.path.join(waiting_room_path, "TOP MONDE*.csv")
    csv_files = glob.glob(pattern)

    if not csv_files:
        print("❌ Aucun fichier CSV 'TOP MONDE' trouvé dans waiting_room")
        return False

    print(f"📁 {len(csv_files)} fichier(s) TOP MONDE trouvé(s) :")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")

    processed_count = 0
    for input_file in csv_files:
        filename = os.path.basename(input_file)
        output_filename = filename.replace(".csv", "_enhanced.csv")
        output_file = os.path.join(ready_to_use_path, output_filename)

        # Vérifier si le fichier enhanced existe déjà
        if os.path.exists(output_file):
            print(f"\n⚠️  Le fichier enhanced existe déjà : {output_filename}")
            print("🔄 Aucune action nécessaire - le fichier est déjà traité.")
            processed_count += 1
            continue

        print(f"\n📂 Traitement du fichier : {filename}")

        try:
            # Lire le fichier CSV
            df = pd.read_csv(input_file)
            print(
                f"📊 Données chargées : {df.shape[0]} lignes × {df.shape[1]} colonnes"
            )

            # Créer les nouvelles colonnes

            # 1. perf_sum : somme des performances 1 an + 6 mois + 3 mois
            perf_columns = [
                "Performance % 1 année",
                "Performance % 6 mois",
                "Performance % 3 mois",
            ]
            df["perf_sum"] = df[perf_columns].sum(axis=1)
            print("✅ Colonne 'perf_sum' créée")

            # 1bis. perf_norm : 1 + perf_sum/1000
            df["perf_norm"] = 1 + df["perf_sum"] / 1000
            print("✅ Colonne 'perf_norm' créée")

            # 2. MRAT : moyenne mobile 21 / moyenne mobile 200
            df["MRAT"] = (
                df["Moyenne mobile simple (21) 1 jour"]
                / df["Moyenne mobile simple (200) 1 jour"]
            )
            print("✅ Colonne 'MRAT' créée")

            # 3. Diff : prix / moyenne mobile 200
            df["Diff"] = df["Prix"] / df["Moyenne mobile simple (200) 1 jour"]
            print("✅ Colonne 'Diff' créée")

            # 4. score : somme de perf_norm et MRAT
            df["score"] = df["perf_norm"] + df["MRAT"]
            print("✅ Colonne 'score' créée")

            # Remplir les valeurs nulles des colonnes calculées par 0
            calculated_columns = ["perf_sum", "perf_norm", "MRAT", "Diff", "score"]
            df[calculated_columns] = df[calculated_columns].fillna(0)
            print("✅ Valeurs nulles des colonnes calculées remplacées par 0")

            # Arrondir toutes les valeurs numériques du DataFrame
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].round(2)
            print("✅ Toutes les valeurs numériques arrondies à 2 décimales")

            # Trier le DataFrame par score décroissant
            df = df.sort_values(by="score", ascending=False)
            print("✅ Données triées par score décroissant")

            # Sauvegarder le fichier avec les nouvelles colonnes
            df.to_csv(output_file, index=False)
            print(f"💾 Fichier sauvegardé : {output_filename}")

            # Afficher un résumé pour ce fichier
            print(f"📈 Résumé pour {filename}:")
            print(f"  - perf_sum - Moyenne: {df['perf_sum'].mean():.2f}%")
            print(f"  - MRAT - Moyenne: {df['MRAT'].mean():.4f}")
            print(f"  - Diff - Moyenne: {df['Diff'].mean():.4f}")
            print(f"  - score - Moyenne: {df['score'].mean():.4f}")

            processed_count += 1

        except Exception as e:
            print(f"❌ Erreur lors du traitement de {filename}: {e}")
            continue

    # Résumé final
    print(f"\n🎉 TRAITEMENT TERMINÉ !")
    print(f"📊 {processed_count} fichier(s) traité(s) sur {len(csv_files)} trouvé(s)")

    return True


if __name__ == "__main__":
    print("🚀 Démarrage de top_monde_ranking...")
    success = top_monde_ranking()
    if success:
        print("\n✅ Analyse TOP MONDE terminée avec succès !")
    else:
        print("\n❌ Échec de l'analyse.")
