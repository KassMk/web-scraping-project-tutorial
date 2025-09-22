import os
from bs4 import BeautifulSoup
import requests
import time
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# ---------------------
# Paso 1: Descargar HTML
# ---------------------
def download_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception(f"Error {resp.status_code} al acceder a {url}")
    print("Página descargada exitosamente")
    return resp.text

# ---------------------
# Paso 2: Parsear tabla
# ---------------------
def parse_table(html):
    tables = pd.read_html(html)
    df = tables[0] 
    
    if "Ref." in df.columns:
        df = df.drop(columns=["Ref."])
    
    df.columns = ["Rank", "Song", "Artist", "Streams", "Release_Date"]

    df = df[~df["Streams"].astype(str).str.contains("As of", na=False)]

    df["Streams"] = df["Streams"].astype(float) * 1e9 
    df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")
    df["Release_Date"] = pd.to_datetime(df["Release_Date"], errors="coerce")

    print(f"Tabla procesada: {len(df)} canciones")
    return df.reset_index(drop=True)

# ---------------------
# Paso 3: Guardar en SQLite
# ---------------------
def save_to_sqlite(df, db_name="spotify.db", table_name="most_streamed_songs"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, con=conn, if_exists="replace", index=False)
    conn.close()
    print(f"Datos guardados en {db_name}, tabla: {table_name}")

# ---------------------
# Paso 4: Cargar desde SQLite
# ---------------------
def load_from_sqlite(db_name="spotify.db", table_name="most_streamed_songs"):

    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    
    df["Release_Date"] = pd.to_datetime(df["Release_Date"], errors="coerce")
    
    return df

# ---------------------
# Paso 5: Generar visualizaciones
# ---------------------
def create_visualizations(df):
    
    # Visualización 1: Top 10 canciones más reproducidas
    print("Gráfico 1: Top 10 canciones...")
    top10 = df.nlargest(10, "Streams")
    plt.figure(figsize=(12, 8))
    plt.barh(top10["Song"], top10["Streams"]/1e9, color="skyblue")
    plt.gca().invert_yaxis()
    plt.title("Top 10 canciones más reproducidas en Spotify", fontsize=16, pad=20)
    plt.xlabel("Streams (billions)", fontsize=12)
    plt.tight_layout()
    plt.show()

    # Visualización 2: Distribución por año de lanzamiento
    print("Gráfico 2: Distribución por año...")
    df["year"] = df["Release_Date"].dt.year
    year_counts = df["year"].value_counts().sort_index()
    plt.figure(figsize=(12, 6))
    year_counts.plot(kind="bar", color="lightgreen")
    plt.title("Número de canciones en la lista por año de lanzamiento", fontsize=16, pad=20)
    plt.xlabel("Año", fontsize=12)
    plt.ylabel("Cantidad de canciones", fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Visualización 3: Top 10 artistas con más streams
    print("Gráfico 3: Top artistas...")
    artist_streams = df.groupby("Artist")["Streams"].sum().nlargest(10)
    plt.figure(figsize=(12, 8))
    artist_streams.plot(kind="bar", color="salmon")
    plt.title("Top 10 artistas por streams totales", fontsize=16, pad=20)
    plt.xlabel("Artista", fontsize=12)
    plt.ylabel("Streams totales", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

# ---------------------
# Función principal
# ---------------------
def main():
    print("=" * 50)
    
    url = "https://en.wikipedia.org/wiki/List_of_most-streamed_songs_on_Spotify"
    
    try:
        html = download_html(url)
        df = parse_table(html)

        print(f"Primeras 3 filas:")
        print(df.head(3))

        save_to_sqlite(df)

        df_viz = load_from_sqlite()
        create_visualizations(df_viz)

        print(f"Base de datos: spotify.db")
        print(f"Canciones procesadas: {len(df)}")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")


if __name__ == "__main__":
    main()
