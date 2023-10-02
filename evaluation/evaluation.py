import pandas as pd
import matplotlib.pyplot as plt

# Carica il file CSV in un DataFrame
df = pd.read_csv("threshold24000.csv")

# Crea un grafico con tutte e tre le "action" sovrapposte
plt.figure(figsize=(100, 60))  # Imposta le dimensioni del grafico

# Per ogni "action", plotta i dati con un colore diverso
for action, group_data in df.groupby("action"):
    plt.scatter(group_data["n"], group_data["time"], label=action)

plt.xlabel("elements in the buffer")  # Etichetta dell'asse X
plt.ylabel("time elapsed")  # Etichetta dell'asse Y
plt.ylim(0, 0.0010)
plt.legend()  # Aggiunge la legenda
plt.grid(True)  # Abilita la griglia
plt.show()  # Mostra il grafico


