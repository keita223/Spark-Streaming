"""
Utilitaires pour la transformation CSV → JSON
et la définition du schéma PySpark pour les transactions.

Fichier source : data/transactions.csv
Colonnes       : transaction_id, user_id, amount, timestamp
"""

import csv
import json
import os


def get_transaction_schema():
    """
    Retourne le schéma PySpark (StructType) pour les données de transactions.
    L'import pyspark est local pour ne pas bloquer le producer (Windows sans Spark).
    """
    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

    return StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id",        IntegerType(), True),
        StructField("amount",         FloatType(),   True),
        StructField("timestamp",      StringType(),  True),
    ])


def csv_to_json_records(csv_path):
    """
    Lit un fichier CSV de transactions et retourne une liste de chaînes JSON,
    une par ligne, prêtes à être envoyées dans Kafka.

    Args:
        csv_path (str): Chemin vers le fichier CSV.

    Returns:
        list[str]: Liste de messages JSON sérialisés.
    """
    records = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {
                "transaction_id": int(row["transaction_id"]),
                "user_id":        int(row["user_id"]),
                "amount":         float(row["amount"]),
                "timestamp":      row["timestamp"].strip(),
            }
            records.append(json.dumps(record))
    return records


def print_sample(csv_path, n=5):
    """
    Affiche les n premiers enregistrements convertis en JSON.
    """
    records = csv_to_json_records(csv_path)
    print(f"Aperçu des {n} premières transactions (JSON) :")
    print("-" * 60)
    for record in records[:n]:
        print(record)
    print("-" * 60)
    print(f"Total : {len(records)} transactions\n")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, "data", "transactions.csv")
    print_sample(csv_path)
