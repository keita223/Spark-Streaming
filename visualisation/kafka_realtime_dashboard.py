"""
Dashboard temps réel connecté directement à Kafka.
Affiche les statistiques des transactions : montants, utilisateurs,
et détecte les transactions à montant élevé (amount > 300).
"""

import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer
import json
import threading

# Configuration
MAX_POINTS = 50
KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC = 'test-topic'
HIGH_VALUE_THRESHOLD = 300

# Données pour les graphiques
timestamps = deque(maxlen=MAX_POINTS)
message_counts = deque(maxlen=MAX_POINTS)
high_value_counts = deque(maxlen=MAX_POINTS)
normal_counts = deque(maxlen=MAX_POINTS)

# Compteurs globaux
total_messages = 0
total_high_value = 0
total_normal = 0
total_amount = 0.0

# Compteurs pour la fenêtre actuelle
current_window_total = 0
current_window_high_value = 0

# Lock pour la synchronisation
data_lock = threading.Lock()


def kafka_consumer_thread():
    """
    Thread qui consomme les transactions Kafka en continu.
    """
    global total_messages, total_high_value, total_normal, total_amount
    global current_window_total, current_window_high_value

    print("Connexion à Kafka...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='dashboard-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
        )

        print(f"Connecté ! En écoute sur le topic '{KAFKA_TOPIC}'...")

        for message in consumer:
            data = message.value
            if not data:
                continue

            amount = data.get("amount", 0)
            user_id = data.get("user_id", "?")

            with data_lock:
                total_messages += 1
                total_amount += amount
                current_window_total += 1

                if amount > HIGH_VALUE_THRESHOLD:
                    total_high_value += 1
                    current_window_high_value += 1
                else:
                    total_normal += 1

            flag = "HIGH" if amount > HIGH_VALUE_THRESHOLD else "ok"
            print(f"[{flag}] user={user_id} | amount={amount:.2f} | total={total_messages}")

    except Exception as e:
        print(f"Erreur Kafka: {e}")


def update_window_stats():
    """
    Enregistre les statistiques de la fenêtre courante et remet les compteurs à zéro.
    """
    global current_window_total, current_window_high_value

    with data_lock:
        timestamps.append(datetime.now())
        message_counts.append(current_window_total)
        high_value_counts.append(current_window_high_value)
        normal_counts.append(current_window_total - current_window_high_value)

        current_window_total = 0
        current_window_high_value = 0


def animate(frame):
    """
    Mise à jour des graphiques à chaque frame.
    """
    update_window_stats()

    plt.clf()
    fig = plt.gcf()
    fig.suptitle(
        f'Kafka Streaming — Dashboard Transactions Temps Réel  (seuil : amount > {HIGH_VALUE_THRESHOLD})',
        fontsize=14, fontweight='bold', color='darkblue'
    )

    avg_amount = (total_amount / total_messages) if total_messages > 0 else 0

    # Graphique 1 : Barres cumulatives
    ax1 = plt.subplot(1, 3, 1)
    if total_messages > 0:
        categories = ['Total', f'High\n(>{HIGH_VALUE_THRESHOLD})', 'Normal']
        values = [total_messages, total_high_value, total_normal]
        colors = ['#3498db', '#e74c3c', '#2ecc71']
        bars = ax1.bar(categories, values, color=colors, alpha=0.8)
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width() / 2., height,
                     f'{int(height)}', ha='center', va='bottom', fontweight='bold')
        ax1.set_title('Transactions cumulatives', fontweight='bold')
        ax1.set_ylabel('Nombre')
        ax1.grid(True, alpha=0.3, axis='y')

    # Graphique 2 : Camembert
    ax2 = plt.subplot(1, 3, 2)
    if total_messages > 0:
        sizes = [total_high_value, total_normal]
        labels = [f'High (>{HIGH_VALUE_THRESHOLD})\n{total_high_value}', f'Normal\n{total_normal}']
        colors = ['#e74c3c', '#2ecc71']
        explode = (0.1, 0)
        ax2.pie(sizes, explode=explode, labels=labels, colors=colors,
                autopct='%1.1f%%', shadow=True, startangle=90)
        ax2.set_title('Distribution des montants', fontweight='bold')

    # Graphique 3 : Statistiques textuelles
    ax3 = plt.subplot(1, 3, 3)
    ax3.axis('off')

    ratio_high = (total_high_value / total_messages * 100) if total_messages > 0 else 0
    ratio_normal = (total_normal / total_messages * 100) if total_messages > 0 else 0
    last_window_total = message_counts[-1] if message_counts else 0
    last_window_high = high_value_counts[-1] if high_value_counts else 0

    stats_text = f"""
    STATISTIQUES TRANSACTIONS

    === CUMULATIF ===
    Total messages :    {total_messages:,}
    High (>{HIGH_VALUE_THRESHOLD}) :     {total_high_value:,}
    Normal :            {total_normal:,}

    Ratio High :        {ratio_high:.1f}%
    Ratio Normal :      {ratio_normal:.1f}%
    Montant moyen :     {avg_amount:.2f}

    === DERNIÈRE FENÊTRE (2s) ===
    Total :             {last_window_total}
    High :              {last_window_high}

    Mise à jour :
    {datetime.now().strftime('%H:%M:%S')}

    Source : topic '{KAFKA_TOPIC}'
    """
    ax3.text(0.05, 0.5, stats_text, fontsize=10, family='monospace',
             verticalalignment='center', color='darkblue')

    plt.tight_layout()


def main():
    print("=" * 60)
    print("DASHBOARD KAFKA — TRANSACTIONS TEMPS RÉEL")
    print("=" * 60)
    print(f"Topic  : {KAFKA_TOPIC}")
    print(f"Server : {KAFKA_SERVER}")
    print(f"Seuil  : amount > {HIGH_VALUE_THRESHOLD}")
    print("=" * 60)
    print("Le producer kafka/producer.py doit être en cours d'exécution.")
    print("Mise à jour toutes les 2 secondes. Fermez la fenêtre pour arrêter.")
    print("=" * 60)

    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()

    import time
    time.sleep(2)

    fig = plt.figure(figsize=(16, 6))
    ani = animation.FuncAnimation(fig, animate, interval=2000, cache_frame_data=False)
    plt.show()


if __name__ == "__main__":
    main()
