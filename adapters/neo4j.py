from neo4j import GraphDatabase
from .base import DatabaseAdapter
from tqdm import tqdm

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password")

BATCH_SIZE = 10000


class Neo4jAdapter(DatabaseAdapter):
    def connect(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    def reset_and_load(self, data):
        with self.driver.session() as session:
            print("Suppression des données existantes...")
            session.run("MATCH ()-[r]->() DELETE r")
            session.run("MATCH (n) DELETE n")

            print("Création des index...")
            session.execute_write(lambda tx: tx.run("CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.id)"))
            session.execute_write(lambda tx: tx.run("CREATE INDEX IF NOT EXISTS FOR (p:Product) ON (p.id)"))

            print("Chargement des utilisateurs...")
            users_data = [{'id': u['id'], 'name': u['name']} for u in data['users']]
            for i in tqdm(range(0, len(users_data), BATCH_SIZE), desc="Users", unit="batch"):
                batch = users_data[i:i + BATCH_SIZE]
                session.execute_write(lambda tx, b=batch: tx.run("""
                UNWIND $batch AS row
                CREATE (:User {id: row.id, name: row.name})
            """, batch=b))

            print("Chargement des produits...")
            products_data = [{'id': p['id'], 'name': p['name']} for p in data['products']]
            for i in tqdm(range(0, len(products_data), BATCH_SIZE), desc="Products", unit="batch"):
                batch = products_data[i:i + BATCH_SIZE]
                session.execute_write(lambda tx, b=batch: tx.run("""
                UNWIND $batch AS row
                CREATE (:Product {id: row.id, name: row.name})
            """, batch=b))

            print("Chargement des relations de suivi...")
            follows_data = [{'follower': f['follower_id'], 'followee': f['followee_id']} for f in data['follows']]
            for i in tqdm(range(0, len(follows_data), BATCH_SIZE), desc="Follows", unit="batch"):
                batch = follows_data[i:i + BATCH_SIZE]
                session.execute_write(lambda tx, b=batch: tx.run("""
                UNWIND $batch AS row
                MERGE (a:User {id: row.follower})
                MERGE (b:User {id: row.followee})
                CREATE (a)-[:FOLLOWS]->(b)
            """, batch=b))

            print("Chargement des achats...")
            purchases_data = [{'uid': p['user_id'], 'pid': p['product_id']} for p in data['purchases']]
            for i in tqdm(range(0, len(purchases_data), BATCH_SIZE), desc="Purchases", unit="batch"):
                batch = purchases_data[i:i + BATCH_SIZE]
                session.execute_write(lambda tx, b=batch: tx.run("""
                UNWIND $batch AS row
                MERGE (u:User {id: row.uid})
                MERGE (p:Product {id: row.pid})
                CREATE (u)-[:BOUGHT]->(p)
            """, batch=b))

            print("Chargement terminé")

    def query_1_products_by_followers(self, user_id, depth):
        query = f"""
        MATCH (influencer:User {{id: $user_id}})<-[:FOLLOWS*1..{depth}]-(follower:User)-[:BOUGHT]->(p:Product)
        RETURN p.name as name, count(DISTINCT follower) as buyers_count
        ORDER BY buyers_count DESC
        """
        with self.driver.session() as session:
            return session.run(query, user_id=user_id).data()

    def query_2_specific_product_influence(self, user_id, product_id, depth):
        query = f"""
        MATCH (influencer:User {{id: $user_id}})<-[:FOLLOWS*1..{depth}]-(follower:User)-[:BOUGHT]->(p:Product {{id: $product_id}})
        RETURN count(DISTINCT follower) as buyers_count
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, product_id=product_id).data()
            if not result:
                return [{'buyers_count': 0}]
            return result

    def query_3_viral_product_disk(self, product_id, level):
        if level == 0:
            query = """
            MATCH (u:User)-[:BOUGHT]->(p:Product {id: $product_id})
            WHERE NOT (u)-[:FOLLOWS]->(:User)-[:BOUGHT]->(p)
            RETURN count(DISTINCT u) as viral_buyers
            """
            with self.driver.session() as session:
                result = session.run(query, product_id=product_id).data()
                return result if result else [{'viral_buyers': 0}]

        organic_query = """
        MATCH (u:User)-[:BOUGHT]->(p:Product {id: $product_id})
        WHERE NOT (u)-[:FOLLOWS]->(:User)-[:BOUGHT]->(p)
        RETURN collect(DISTINCT u.id) as ids
        """
        hop_query = """
        MATCH (follower:User)-[:FOLLOWS]->(u:User)
        WHERE u.id IN $current_ids
          AND (follower)-[:BOUGHT]->(:Product {id: $product_id})
          AND NOT follower.id IN $visited_ids
        RETURN collect(DISTINCT follower.id) as ids
        """
        with self.driver.session() as session:
            organic_ids = session.run(organic_query, product_id=product_id).single()['ids']
            visited = set(organic_ids)
            current = set(organic_ids)
            disk_buyers = set()

            for _ in range(level):
                if not current:
                    break
                new_ids = session.run(
                    hop_query,
                    current_ids=list(current),
                    visited_ids=list(visited),
                    product_id=product_id
                ).single()['ids']
                new_set = set(new_ids) - visited
                visited |= new_set
                current = new_set
                disk_buyers |= new_set

            return [{'viral_buyers': len(disk_buyers)}]

    def query_4_viral_product_circle(self, product_id, level):
        if level == 0:
            query = """
            MATCH (u:User)-[:BOUGHT]->(p:Product {id: $product_id})
            WHERE NOT (u)-[:FOLLOWS]->(:User)-[:BOUGHT]->(p)
            RETURN count(DISTINCT u) as viral_buyers
            """
            with self.driver.session() as session:
                result = session.run(query, product_id=product_id).data()
                return result if result else [{'viral_buyers': 0}]

        organic_query = """
        MATCH (u:User)-[:BOUGHT]->(p:Product {id: $product_id})
        WHERE NOT (u)-[:FOLLOWS]->(:User)-[:BOUGHT]->(p)
        RETURN collect(DISTINCT u.id) as ids
        """
        hop_query = """
        MATCH (follower:User)-[:FOLLOWS]->(u:User)
        WHERE u.id IN $current_ids
          AND (follower)-[:BOUGHT]->(:Product {id: $product_id})
          AND NOT follower.id IN $visited_ids
        RETURN collect(DISTINCT follower.id) as ids
        """
        with self.driver.session() as session:
            organic_ids = session.run(organic_query, product_id=product_id).single()['ids']
            visited = set(organic_ids)
            current = set(organic_ids)

            for _ in range(level):
                if not current:
                    return [{'viral_buyers': 0}]
                new_ids = session.run(
                    hop_query,
                    current_ids=list(current),
                    visited_ids=list(visited),
                    product_id=product_id
                ).single()['ids']
                new_set = set(new_ids) - visited
                visited |= new_set
                current = new_set

            return [{'viral_buyers': len(current)}]


    def get_stats(self):
        try:
            with self.driver.session() as session:
                users = session.run("MATCH (u:User) RETURN count(u) as cnt").single()['cnt']
                products = session.run("MATCH (p:Product) RETURN count(p) as cnt").single()['cnt']
                follows = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as cnt").single()['cnt']
                purchases = session.run("MATCH ()-[r:BOUGHT]->() RETURN count(r) as cnt").single()['cnt']
            return {'users': users, 'products': products, 'follows': follows, 'purchases': purchases}
        except Exception:
            return None

    def close(self):
        self.driver.close()

