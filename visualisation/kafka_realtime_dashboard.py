"""
Dashboard temps reel connecte directement a Kafka
Affiche les vraies statistiques des messages
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

# Donnees pour les graphiques
timestamps = deque(maxlen=MAX_POINTS)
message_counts = deque(maxlen=MAX_POINTS)
important_counts = deque(maxlen=MAX_POINTS)
regular_counts = deque(maxlen=MAX_POINTS)

# Compteurs globaux
total_messages = 0
total_important = 0
total_regular = 0

# Compteurs pour la fenetre actuelle
current_window_total = 0
current_window_important = 0

# Lock pour la synchronisation
data_lock = threading.Lock()

def kafka_consumer_thread():
    """
    Thread qui consomme les messages Kafka en continu
    """
    global total_messages, total_important, total_regular
    global current_window_total, current_window_important

    print("Connexion a Kafka...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='latest',  # Commence par les nouveaux messages
            enable_auto_commit=True,
            group_id='dashboard-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
        )

        print(f"Connecte a Kafka! En ecoute sur le topic '{KAFKA_TOPIC}'...")

        for message in consumer:
            if message.value:
                value = str(message.value)

                with data_lock:
                    total_messages += 1
                    current_window_total += 1

                    if 'important' in value.lower():
                        total_important += 1
                        current_window_important += 1
                    else:
                        total_regular += 1

                print(f"Message recu: {value[:50]}... (Total: {total_messages})")

    except Exception as e:
        print(f"Erreur Kafka: {e}")

def update_window_stats():
    """
    Met a jour les statistiques pour la fenetre actuelle
    """
    global current_window_total, current_window_important

    with data_lock:
        timestamps.append(datetime.now())
        message_counts.append(current_window_total)
        important_counts.append(current_window_important)
        regular_counts.append(current_window_total - current_window_important)

        # Reset pour la prochaine fenetre
        current_window_total = 0
        current_window_important = 0

def animate(frame):
    """
    Fonction appelee a chaque frame pour mettre a jour les graphiques
    """
    # Mettre a jour les stats de la fenetre
    update_window_stats()

    # Nettoyer les graphiques
    plt.clf()

    fig = plt.gcf()
    fig.suptitle('Kafka Streaming - Dashboard Temps Reel (VRAIES DONNEES)',
                 fontsize=16, fontweight='bold', color='green')

    # Graphique 1: Evolution cumulative
    ax1 = plt.subplot(1, 3, 1)
    if total_messages > 0:
        categories = ['Total', 'Important', 'Regular']
        values = [total_messages, total_important, total_regular]
        colors = ['#3498db', '#2ecc71', '#e74c3c']
        bars = ax1.bar(categories, values, color=colors, alpha=0.7)

        # Ajouter les valeurs sur les barres
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontweight='bold')

        ax1.set_title('Messages Cumulatifs', fontweight='bold')
        ax1.set_ylabel('Nombre total')
        ax1.grid(True, alpha=0.3, axis='y')

    # Graphique 2: Camembert
    ax2 = plt.subplot(1, 3, 2)
    if total_messages > 0:
        sizes = [total_important, total_regular]
        labels = [f'Important\n{total_important}', f'Regular\n{total_regular}']
        colors = ['#2ecc71', '#e74c3c']
        explode = (0.1, 0)

        ax2.pie(sizes, explode=explode, labels=labels, colors=colors,
                autopct='%1.1f%%', shadow=True, startangle=90)
        ax2.set_title('Distribution Globale', fontweight='bold')

    # Graphique 3: Statistiques textuelles
    ax3 = plt.subplot(1, 3, 3)
    ax3.axis('off')

    ratio_important = (total_important / total_messages * 100) if total_messages > 0 else 0
    ratio_regular = (total_regular / total_messages * 100) if total_messages > 0 else 0

    # Derniere fenetre
    last_window_total = message_counts[-1] if len(message_counts) > 0 else 0
    last_window_important = important_counts[-1] if len(important_counts) > 0 else 0

    stats_text = f"""
    STATISTIQUES KAFKA EN TEMPS REEL

    === CUMULATIVE ===
    Total Messages:     {total_messages:,}
    Messages Important: {total_important:,}
    Messages Regular:   {total_regular:,}

    Ratio Important:    {ratio_important:.1f}%
    Ratio Regular:      {ratio_regular:.1f}%

    === DERNIERE FENETRE (2s) ===
    Total:              {last_window_total}
    Important:          {last_window_important}

    Derniere mise a jour:
    {datetime.now().strftime('%H:%M:%S')}

    SOURCE: Kafka topic '{KAFKA_TOPIC}'
    """

    ax3.text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
             verticalalignment='center', color='darkgreen')

    plt.tight_layout()

def main():
    """
    Lance le dashboard de visualisation
    """
    print("=" * 60)
    print("DASHBOARD KAFKA TEMPS REEL - VRAIES DONNEES")
    print("=" * 60)
    print(f"Topic Kafka: {KAFKA_TOPIC}")
    print(f"Server Kafka: {KAFKA_SERVER}")
    print("=" * 60)
    print("")
    print("IMPORTANT: Le producteur Kafka doit etre en cours d'execution!")
    print("")
    print("Le dashboard se met a jour toutes les 2 secondes")
    print("Fermez la fenetre pour arreter")
    print("=" * 60)

    # Demarrer le thread Kafka consumer
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()

    # Attendre un peu que le consumer se connecte
    import time
    time.sleep(2)

    # Creer la figure
    fig = plt.figure(figsize=(16, 10))

    # Animation: mise a jour toutes les 2000ms (2 secondes)
    ani = animation.FuncAnimation(fig, animate, interval=2000, cache_frame_data=False)

    plt.show()

if __name__ == "__main__":
    main()
