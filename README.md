
# aviv_etl – Pipeline ETL PySpark vers PostgreSQL

Projet **config-driven** pour ingérer un jeu de données *listings* (CSV), appliquer des transformations (SQL ou Python),
faire des **contrôles qualité** (Great Expectations) puis **charger** les données en **Parquet** (silver) et en **PostgreSQL**
(gold) dans les tables  modélisé comme `property`, `historique_price` etc .

---

## 🧭 Vue d’ensemble

- **Langage** : Python
- **Moteur** : PySpark (Spark local `local[4]`)
- **Qualité des données** : Great Expectations (validations paramétrables)
- **Stockage** :
  - **RAW / SILVER / GOLD** : chemins paramétrés via `.env`
  - **Parquet** (silver) avec partition par `eventdate`
  - **PostgreSQL** (gold) via JDBC
- **Exécution** : `python local.py` (choisit un job YAML dans `config/`)

---

## 🗂️ Structure du repo

```
aviv_etl/
├─ local.py                       # point d’entrée : lance un pipeline à partir d’un YAML
├─ src/
│  ├─ execution_context.py        # lit .env + YAML, résout les chemins, instancie Spark
│  ├─ wrapper_pipeline.py         # enchaîne les jobs (GroupConfig) + timing & statut
│  ├─ pipeline.py                 # orchestration d’un job (inputs → transforms → quality_gate → outputs)
│  ├─ extract/                    # Extractor générique (CSV/...) : schéma DDL, options Spark
│  ├─ transform/                  # SQL ou fonctions Python (freestyle): normalisation, utils
│  ├─ load/                       # loaders Parquet & PostgreSQL (JDBC)
│  ├─ validators/                 # Great Expectations – contrôles colonnes/règles
│  └─ utils/                      # Spark helper + lecture des fichiers YAML
├─ config/
│  ├─ silver/listing.yaml         # job silver : RAW → nettoyage → Parquet (partition eventdate)
│  └─ gold/listing.yaml           # job gold : silver → quality gate → Postgres
├─ initdb/init_listing_table.sql  # DDL PostgreSQL (property, historique_price, dictionnaires)
├─ docker-compose.yaml            # Postgres + pgAdmin
├─ .env                           # variables (DB_*, RAW, SILVER, GOLD)
├─ requirements.in
└─ requirements.txt
```

---

## 🔧 Prérequis

- **Python** ≥ 3.11 recommandé
- **Java JDK** ≥ 11 (requis par PySpark)
- **PostgreSQL** (local ou via Docker Compose)
- **pip** & **virtualenv** (recommandé)

Paquets Python utiles (selon le code du projet) :
`pyspark`, `dataclasses-json`, `python-dotenv`, `great-expectations`, `pandas`, `pyyaml`

---

## ⚙️ Configuration (.env)

Exemple de `.env` à la racine du projet :

```env
# --- Base de données ---
DB_USER=db
DB_PASSWORD=db
DB_NAME=postgres_db
DB_HOST=localhost
DB_PORT=5432

# --- Data Lake local ---
RAW=./datalake/raw
SILVER=./datalake/silver
GOLD=./datalake/gold
```

> Créez les dossiers `./datalake/{raw,silver,gold}` et placez votre `listings.csv` dans `${RAW}`.

---

## 🐘 Base de données (Docker)

Un `docker-compose.yaml` est fourni pour Postgres + pgAdmin. Démarrer :

```bash
docker compose up -d
# Vérifier la santé
docker inspect --format='{{.State.Health.Status}}' postgres_db
```

Initialiser le schéma (tables `property`, `historique_price`, etc.) :

```bash
# Méthode 1 : psql local
psql "postgres://db:db@localhost:5432/postgres_db" -f initdb/init_listing_table.sql

# Méthode 2 : via le conteneur
docker exec -i postgres_db psql -U db -d postgres_db < initdb/init_listing_table.sql
```

pgAdmin : http://localhost:5050 (admin@admin.com / admin). Ajoutez un serveur vers `postgres_db:5432` (user: `db`, pass: `db`).

---

## 📦 Installation

```bash
python -m venv .venv
source .venv/bin/activate          # (Windows) .venv\Scripts\activate
pip install --upgrade pip

# Dépendances projet
pip install -r requirements.txt    # (ou pip install -r requirements.in)

# Dépendances souvent manquantes
pip install pyspark dataclasses-json python-dotenv great-expectations
```


---

## ▶️ Exécution

1) Vérifiez `.env` (chemins RAW/SILVER/GOLD + DB).  
2) Placez **`listings.csv`** dans `${RAW}`.  
3) Sélectionnez le job dans `local.py` :

```python
# local.py
context = ExecutionContext(
    # specification_file="config/silver/listing.yaml"
    specification_file="config/gold/listing.yaml"
)
```

- **Silver** : lit `${RAW}/listings.csv`, nettoie/normalise, écrit en **Parquet** dans `${SILVER}/listing` (partition `eventdate=YYYY-MM-DD`).  
- **Gold** : lit la silver du jour (ex: `${SILVER}/listing/eventdate=2025-08-26`), exécute **quality gate**, charge Postgres dans **`property`** et **`historique_price`**.

Lancez :

```bash
python local.py
```

> Le Spark local est généralement configuré sur `local[4]`, mémoire driver 4g.  
> Le connecteur JDBC Postgres peut être chargé via `spark.jars.packages=org.postgresql:postgresql:42.7.4`.

---

## 🧪 Contrôles qualité (Great Expectations)

La section `quality_gate` des YAML (ex: `config/gold/listing.yaml`) déclenche un validateur (présence / nullité / types / cardinalités).  
Les résultats peuvent être exportés (events JSON).  
Pour désactiver : commentez la section `quality_gate` du YAML gold.

---

## 🧱 Schéma de données (Gold / PostgreSQL)

Le script `initdb/init_listing_table.sql` gère :

- Tables : `transaction_type`, `item_type`, `item_sub_type`
- **`property`** (colonnes : `id_property`, `start_date`, `price`, `area`, `site_area`, `floor`, `room_count`, `balcony_count`, `terrace_count`, `has_garden`, `city`, `zipcode`, `has_passenger_lift`, `is_new_construction`, `build_year`, `terrace_area`, `has_cellar`, `id_transaction_type`, `id_item_type`, `id_item_sub_type`)
- **`historique_price`** (suivi des changements de prix par `id_property`)


---

## 🧩 YAML de pipeline (structure)

Chaque job YAML contient :

- **`inputs`** : sources (format, options Spark, schéma DDL, filtrage, sélection de colonnes)
- **`transforms`** : transformations en chaîne (SQL ou Python *freestyle*)
- **`quality_gate`** *(optionnel)* : validations Great Expectations
- **`outputs`** : loaders (Parquet et/ou Postgres)

Plusieurs jobs peuvent être regroupés (GroupConfig) et exécutés séquentiellement par un wrapper.

---

## 🧰 Débogage & problèmes fréquents

- **`ModuleNotFoundError: No module named 'pyspark'`** : installez `pyspark` et vérifiez **Java (JDK 11+)** + `JAVA_HOME`.
- **`great_expectations` introuvable** : `pip install great-expectations` ou commentez `quality_gate`.
- **Connexion Postgres refusée** : vérifiez Docker et `.env` (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`).
- **Chemins RAW/SILVER/GOLD** : créez les dossiers, placez `listings.csv` sous `${RAW}`.

---





## ✅ Récapitulatif rapide

```bash
# 1) Préparer l’env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install pyspark dataclasses-json python-dotenv great-expectations

# 2) Démarrer Postgres
docker compose up -d
psql "postgres://db:db@localhost:5432/postgres_db" 

# 3) Configurer
# Éditez .env (RAW/SILVER/GOLD + DB_*)

# 4) Lancer
python local.py
```

