import json
import time
import random
from adapters import MariaDBAdapter, Neo4jAdapter


def load_data(filepath='dataset.json'):
    with open(filepath, 'r') as f:
        return json.load(f)


def generate_synthetic_data(num_users=1_000_000, num_products=10_000, max_followers=20):
    print("Génération du dataset synthétique...")
    print(f"  {num_users:,} utilisateurs, {num_products:,} produits, 0-{max_followers} followers")

    users = [{'id': i, 'name': f'User_{i}'} for i in range(1, num_users + 1)]
    products = [{'id': i, 'name': f'Product_{i}'} for i in range(1, num_products + 1)]

    print("  Génération des follows...")
    follows = []
    for user_id in range(1, num_users + 1):
        num_following = random.randint(0, max_followers)
        followees = random.sample(range(1, num_users + 1), min(num_following, num_users - 1))
        for followee_id in followees:
            if followee_id != user_id:
                follows.append({'follower_id': user_id, 'followee_id': followee_id})

    print("  Génération des achats...")
    purchases = []
    for user_id in range(1, num_users + 1):
        num_purchases = random.randint(0, min(5, num_products))
        bought_products = random.sample(range(1, num_products + 1), num_purchases)
        for product_id in bought_products:
            purchases.append({'user_id': user_id, 'product_id': product_id})

    print(f"  {len(follows):,} follows, {len(purchases):,} achats générés")
    return {'users': users, 'products': products, 'follows': follows, 'purchases': purchases}


def clear_screen():
    print("\n" * 2)


def pause():
    input("\n[Appuyez sur Entrée pour continuer...]")


def input_int(prompt, default=None):
    while True:
        val = input(f"{prompt} [{default}]: ").strip()
        if not val and default is not None:
            return default
        try:
            return int(val)
        except ValueError:
            print("Veuillez entrer un nombre valide.")


class App:
    def __init__(self):
        self.mariadb = None
        self.neo4j = None
        self.data = None
        self.data_source = None
        self.load_times = {'MariaDB': None, 'Neo4j': None}
        self.enabled = {'MariaDB': True, 'Neo4j': True}
        self._init_databases()

    def _init_databases(self):
        print("=" * 50)
        print("   Connexion aux bases de données...")
        print("=" * 50)

        print("\nConnexion à MariaDB...")
        try:
            self.mariadb = MariaDBAdapter()
            self.mariadb.connect()
            print("  ✓ MariaDB connecté")
        except Exception as e:
            print(f"  ✗ MariaDB erreur"
                  f": {e}")
            self.mariadb = None

        print("\nConnexion à Neo4j...")
        try:
            self.neo4j = Neo4jAdapter()
            self.neo4j.connect()
            print("  ✓ Neo4j connecté")
        except Exception as e:
            print(f"  ✗ Neo4j erreur: {e}")
            self.neo4j = None

        self._detect_existing_data()
        pause()

    def _detect_existing_data(self):
        stats = None
        source_db = None

        for db_name, db in [("MariaDB", self.mariadb), ("Neo4j", self.neo4j)]:
            if db:
                s = db.get_stats()
                if s and s['users'] > 0:
                    stats = s
                    source_db = db_name
                    break

        if stats:
            print(f"\n  ℹ Données existantes détectées dans {source_db}:")
            print(f"    {stats['users']:,} users | {stats['products']:,} produits")
            print(f"    {stats['follows']:,} follows | {stats['purchases']:,} achats")
            print("  → Dataset chargé automatiquement depuis les bases.")
            self.data = True
            self.data_source = f"BDD existante ({stats['users']:,} users via {source_db})"
        else:
            print("\n  ℹ Aucune donnée trouvée dans les bases. Veuillez charger un dataset.")

    def menu_principal(self):
        while True:
            clear_screen()
            print("=" * 50)
            print("   COMPARAISON SQL / NoSQL - Réseau Social")
            print("=" * 50)
            status_maria = "✓" if self.mariadb else "✗"
            status_neo4j = "✓" if self.neo4j else "✗"
            enabled_maria = "ON" if self.enabled['MariaDB'] else "OFF"
            enabled_neo4j = "ON" if self.enabled['Neo4j'] else "OFF"
            time_maria = f" ({self.load_times['MariaDB']:.2f}s)" if self.load_times['MariaDB'] else ""
            time_neo4j = f" ({self.load_times['Neo4j']:.2f}s)" if self.load_times['Neo4j'] else ""
            print(f"\n   MariaDB: {status_maria} [{enabled_maria}]{time_maria}  |  Neo4j: {status_neo4j} [{enabled_neo4j}]{time_neo4j}")
            print(f"   Dataset: {self.data_source or 'Non chargé'}\n")
            print("   1. Choisir et charger le dataset")
            print("   2. Exécuter une requête")
            print("   3. Activer/Désactiver une base de données")
            print("   0. Quitter")
            print()

            choix = input("Votre choix: ").strip()

            if choix == '1':
                self.menu_dataset()
            elif choix == '2':
                self.menu_requetes()
            elif choix == '3':
                self.menu_toggle_db()
            elif choix == '0':
                self.quitter()
                break
            else:
                print("Choix invalide.")
                pause()

    def menu_dataset(self):
        clear_screen()
        print("=" * 50)
        print("   CHOIX DU DATASET")
        print("=" * 50)
        print("\n   1. Utiliser les données existantes en base")
        print("   2. Dataset fichier (dataset.json)")
        print("   3. Dataset synthétique (1M users, 10K produits)")
        print("   0. Retour")
        print()

        choix = input("Votre choix: ").strip()

        if choix == '1':
            self.charger_dataset_base_existante()
        elif choix == '2':
            self.charger_dataset_fichier()
        elif choix == '3':
            self.charger_dataset_synthetique()
        elif choix == '0':
            return

    def charger_dataset_base_existante(self):
        clear_screen()
        print("Vérification des données existantes dans les bases...")
        stats = None
        source_db = None
        for db_name, db in [("MariaDB", self.mariadb), ("Neo4j", self.neo4j)]:
            if db:
                s = db.get_stats()
                if s and s['users'] > 0:
                    stats = s
                    source_db = db_name
                    break

        if stats:
            self.data = True
            self.data_source = f"BDD existante ({stats['users']:,} users via {source_db})"
            print(f"\n  ✓ Données trouvées dans {source_db}:")
            print(f"    {stats['users']:,} users | {stats['products']:,} produits")
            print(f"    {stats['follows']:,} follows | {stats['purchases']:,} achats")
            print(f"\n  Dataset actif : {self.data_source}")
        else:
            print("\n  ✗ Aucune donnée trouvée dans les bases. Veuillez charger un fichier ou un dataset synthétique.")
        pause()

    def charger_dataset_fichier(self):
        clear_screen()
        print("Chargement du dataset depuis fichier...")
        try:
            self.data = load_data()
            self.data_source = f"Fichier ({len(self.data['users'])} users)"
            self._charger_bases()
        except Exception as e:
            print(f"Erreur: {e}")
            pause()

    def charger_dataset_synthetique(self):
        clear_screen()
        num_users = input_int("Nombre d'utilisateurs", 1_000_000)
        num_products = input_int("Nombre de produits", 10_000)
        max_followers = input_int("Max followers par user", 20)

        try:
            self.data = generate_synthetic_data(num_users, num_products, max_followers)
            self.data_source = f"Synthétique ({num_users:,} users)"
            self._charger_bases()
        except Exception as e:
            print(f"Erreur: {e}")
            pause()

    def _charger_bases(self):
        stats = {
            'users': len(self.data['users']),
            'products': len(self.data['products']),
            'follows': len(self.data['follows']),
            'purchases': len(self.data['purchases'])
        }

        print("\n" + "═" * 50)
        print("   IMPORT DES DONNÉES (séquentiel)")
        print("═" * 50)
        print(f"\n   {stats['users']:,} users | {stats['products']:,} produits")
        print(f"   {stats['follows']:,} follows | {stats['purchases']:,} achats")
        print("\n" + "─" * 50)

        print(f"\n{'Base':<12} {'Temps':>10} {'Users/s':>12} {'Follows/s':>12}")
        print("─" * 50)

        if self.mariadb:
            try:
                start = time.time()
                self.mariadb.reset_and_load(self.data)
                elapsed = time.time() - start
                self.load_times['MariaDB'] = elapsed
                users_per_sec = stats['users'] / elapsed
                follows_per_sec = stats['follows'] / elapsed
                print(f"{'MariaDB':<12} {elapsed:>9.2f}s {users_per_sec:>11,.0f} {follows_per_sec:>11,.0f}")
            except Exception as e:
                print(f"{'MariaDB':<12} ERREUR: {e}")
                self.load_times['MariaDB'] = None

        if self.neo4j:
            try:
                start = time.time()
                self.neo4j.reset_and_load(self.data)
                elapsed = time.time() - start
                self.load_times['Neo4j'] = elapsed
                users_per_sec = stats['users'] / elapsed
                follows_per_sec = stats['follows'] / elapsed
                print(f"{'Neo4j':<12} {elapsed:>9.2f}s {users_per_sec:>11,.0f} {follows_per_sec:>11,.0f}")
            except Exception as e:
                print(f"{'Neo4j':<12} ERREUR: {e}")
                self.load_times['Neo4j'] = None

        print("─" * 50)

        if self.load_times['MariaDB'] and self.load_times['Neo4j']:
            if self.load_times['MariaDB'] < self.load_times['Neo4j']:
                ratio = self.load_times['Neo4j'] / self.load_times['MariaDB']
                print(f"\nMariaDB {ratio:.1f}x plus rapide pour l'import")
            else:
                ratio = self.load_times['MariaDB'] / self.load_times['Neo4j']
                print(f"\nNeo4j {ratio:.1f}x plus rapide pour l'import")

        pause()

    def menu_requetes(self):
        if not self.data:
            print("\nVeuillez d'abord charger un dataset.")
            pause()
            return

        while True:
            clear_screen()
            print("=" * 50)
            print("   EXÉCUTER UNE REQUÊTE")
            print("=" * 50)
            print("\n   1. Produits achetés par le réseau de followers (influence)")
            print("   2. Influence sur un produit spécifique (post)")
            print("   3. Viralité d'un produit (disque orienté niveau n)")
            print("   4. Viralité d'un produit (cercle orienté niveau n)")
            print("   0. Retour")
            print()

            choix = input("Votre choix: ").strip()

            if choix == '1':
                self.executer_query(1)
            elif choix == '2':
                self.executer_query(2)
            elif choix == '3':
                self.executer_query(3)
            elif choix == '4':
                self.executer_query(4)
            elif choix == '0':
                break

    def executer_query(self, query_num):
        clear_screen()

        if query_num == 1:
            print("--- Produits achetés par le réseau de followers ---\n")
            user_id = input_int("ID utilisateur", 1)
            depth = input_int("Profondeur (niveau 1 à n)", 2)
            params = (user_id, depth)
        elif query_num == 2:
            print("--- Influence sur un produit spécifique (post) ---\n")
            user_id = input_int("ID utilisateur", 1)
            product_id = input_int("ID produit", 1)
            depth = input_int("Profondeur (niveau 1 à n)", 2)
            params = (user_id, product_id, depth)
        elif query_num == 3:
            print("--- Viralité d'un produit (disque orienté niveau n) ---\n")
            product_id = input_int("ID produit", 1)
            level = input_int("Niveau exact (0, 1, 2...)", 2)
            params = (product_id, level)
        elif query_num == 4:
            print("--- Viralité d'un produit (cercle orienté niveau n) ---\n")
            product_id = input_int("ID produit", 1)
            level = input_int("Niveau exact (0, 1, 2...)", 2)
            params = (product_id, level)

        print("\n" + "─" * 50)
        print("Exécution (séquentiel)...")

        def run_query(db_name, db):
            start = time.time()
            if query_num == 1:
                result = db.query_1_products_by_followers(*params)
            elif query_num == 2:
                result = db.query_2_specific_product_influence(*params)
            elif query_num == 3:
                result = db.query_3_viral_product_disk(*params)
            elif query_num == 4:
                result = db.query_4_viral_product_circle(*params)
            elapsed = time.time() - start
            return db_name, result, elapsed

        results = {}
        for db_name, db in [("MariaDB", self.mariadb), ("Neo4j", self.neo4j)]:
            if db and self.enabled[db_name]:
                try:
                    db_name, result, elapsed = run_query(db_name, db)
                    results[db_name] = {'result': result, 'elapsed': elapsed}
                except Exception as e:
                    results[db_name] = {'error': str(e)}

        for db_name in ["MariaDB", "Neo4j"]:
            db_obj = self.mariadb if db_name == "MariaDB" else self.neo4j
            if not db_obj:
                print(f"\n{db_name}: Non connecté")
                continue
            if not self.enabled[db_name]:
                print(f"\n{db_name}: Désactivé")
                continue
            if db_name not in results:
                print(f"\n{db_name}: Non disponible")
                continue

            print(f"\n{db_name}:")
            r = results[db_name]
            if 'error' in r:
                print(f"  Erreur: {r['error']}")
            else:
                result, elapsed = r['result'], r['elapsed']
                if query_num == 1:
                    for item in result[:10]:
                        print(f"  • {item['name']}: {item['buyers_count']} acheteurs")
                    if len(result) > 10:
                        print(f"  ... et {len(result) - 10} autres")
                elif query_num == 2:
                    print(f"  → Acheteurs influencés: {result[0]['buyers_count']}")
                elif query_num in (3, 4):
                    print(f"  → Acheteurs viraux au niveau {params[1]}: {result[0]['viral_buyers']}")
                print(f"  {elapsed:.4f}s")

        pause()

    def menu_toggle_db(self):
        while True:
            clear_screen()
            print("=" * 50)
            print("   ACTIVER / DÉSACTIVER UNE BASE DE DONNÉES")
            print("=" * 50)
            maria_state = "ON ✓" if self.enabled['MariaDB'] else "OFF ✗"
            neo4j_state = "ON ✓" if self.enabled['Neo4j'] else "OFF ✗"
            print(f"\n   1. MariaDB  [{maria_state}]")
            print(f"   2. Neo4j    [{neo4j_state}]")
            print("   0. Retour")
            print()

            choix = input("Votre choix: ").strip()

            if choix == '1':
                self.enabled['MariaDB'] = not self.enabled['MariaDB']
                state = "activé" if self.enabled['MariaDB'] else "désactivé"
                print(f"  → MariaDB {state}.")
                pause()
            elif choix == '2':
                self.enabled['Neo4j'] = not self.enabled['Neo4j']
                state = "activé" if self.enabled['Neo4j'] else "désactivé"
                print(f"  → Neo4j {state}.")
                pause()
            elif choix == '0':
                break
            else:
                print("Choix invalide.")
                pause()

    def quitter(self):
        if self.mariadb:
            self.mariadb.close()
        if self.neo4j:
            self.neo4j.close()


def main():
    app = App()
    app.menu_principal()


if __name__ == '__main__':
    main()


