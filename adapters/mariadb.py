import mysql.connector
from .base import DatabaseAdapter
from tqdm import tqdm

MARIADB_CONFIG = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'social_network',
    'port': 3306
}

BATCH_SIZE = 10000


class MariaDBAdapter(DatabaseAdapter):
    def connect(self):
        self.conn = mysql.connector.connect(**MARIADB_CONFIG)
        self.cursor = self.conn.cursor(dictionary=True)

    def reset_and_load(self, data):
        print("Suppression des tables existantes...")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.cursor.execute("DROP TABLE IF EXISTS purchases")
        self.cursor.execute("DROP TABLE IF EXISTS follows")
        self.cursor.execute("DROP TABLE IF EXISTS products")
        self.cursor.execute("DROP TABLE IF EXISTS users")

        print("Création des tables...")
        self.cursor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))")
        self.cursor.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255))")
        self.cursor.execute("""
                            CREATE TABLE follows (
                                                     follower_id INT,
                                                     followee_id INT,
                                                     PRIMARY KEY (follower_id, followee_id),
                                                     INDEX idx_followee (followee_id),
                                                     INDEX idx_follower (follower_id)
                            )
                            """)
        self.cursor.execute("""
                            CREATE TABLE purchases (
                                                       user_id INT,
                                                       product_id INT,
                                                       PRIMARY KEY (user_id, product_id),
                                                       INDEX idx_product (product_id),
                                                       INDEX idx_user (user_id)
                            )
                            """)

        print("Chargement des utilisateurs...")
        users_data = [(u['id'], u['name']) for u in data['users']]
        for i in tqdm(range(0, len(users_data), BATCH_SIZE), desc="Users", unit="batch"):
            batch = users_data[i:i + BATCH_SIZE]
            self.cursor.executemany("INSERT INTO users (id, name) VALUES (%s, %s)", batch)
            self.conn.commit()

        print("Chargement des produits...")
        products_data = [(p['id'], p['name']) for p in data['products']]
        for i in tqdm(range(0, len(products_data), BATCH_SIZE), desc="Products", unit="batch"):
            batch = products_data[i:i + BATCH_SIZE]
            self.cursor.executemany("INSERT INTO products (id, name) VALUES (%s, %s)", batch)
            self.conn.commit()

        print("Chargement des relations de suivi...")
        follows_data = [(f['follower_id'], f['followee_id']) for f in data['follows']]
        for i in tqdm(range(0, len(follows_data), BATCH_SIZE), desc="Follows", unit="batch"):
            batch = follows_data[i:i + BATCH_SIZE]
            self.cursor.executemany("INSERT INTO follows (follower_id, followee_id) VALUES (%s, %s)", batch)
            self.conn.commit()

        print("Chargement des achats...")
        purchases_data = [(p['user_id'], p['product_id']) for p in data['purchases']]
        for i in tqdm(range(0, len(purchases_data), BATCH_SIZE), desc="Purchases", unit="batch"):
            batch = purchases_data[i:i + BATCH_SIZE]
            self.cursor.executemany("INSERT INTO purchases (user_id, product_id) VALUES (%s, %s)", batch)
            self.conn.commit()

        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.conn.commit()
        print("Chargement terminé")

    def query_1_products_by_followers(self, user_id, depth):
        query = f"""
        WITH RECURSIVE UserNetwork AS (
            SELECT follower_id, 1 as level
            FROM follows
            WHERE followee_id = {user_id}
            UNION ALL
            SELECT f.follower_id, un.level + 1
            FROM follows f
            INNER JOIN UserNetwork un ON f.followee_id = un.follower_id
            WHERE un.level < {depth}
        )
        SELECT p.name, COUNT(DISTINCT un.follower_id) as buyers_count
        FROM UserNetwork un
        JOIN purchases pur ON un.follower_id = pur.user_id
        JOIN products p ON pur.product_id = p.id
        GROUP BY p.id;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_2_specific_product_influence(self, user_id, product_id, depth):
        query = f"""
        WITH RECURSIVE UserNetwork AS (
            SELECT follower_id, 1 as level
            FROM follows
            WHERE followee_id = {user_id}
            UNION ALL
            SELECT f.follower_id, un.level + 1
            FROM follows f
            INNER JOIN UserNetwork un ON f.followee_id = un.follower_id
            WHERE un.level < {depth}
        )
        SELECT COUNT(DISTINCT un.follower_id) as buyers_count
        FROM UserNetwork un
        JOIN purchases pur ON un.follower_id = pur.user_id
        WHERE pur.product_id = {product_id};
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_3_viral_product_disk(self, product_id, level):
        if level == 0:
            query = f"""
            SELECT COUNT(DISTINCT p1.user_id) as viral_buyers
            FROM purchases p1
            WHERE p1.product_id = {product_id}
              AND NOT EXISTS (
                SELECT 1 FROM follows f
                JOIN purchases p2 ON f.followee_id = p2.user_id
                WHERE f.follower_id = p1.user_id AND p2.product_id = {product_id}
              );
            """
        else:
            query = f"""
            WITH RECURSIVE chain AS (
                SELECT p1.user_id AS current_id, 0 AS lvl
                FROM purchases p1
                WHERE p1.product_id = {product_id}
                  AND NOT EXISTS (
                    SELECT 1 FROM follows f_init
                    JOIN purchases p_init ON f_init.followee_id = p_init.user_id
                    WHERE f_init.follower_id = p1.user_id AND p_init.product_id = {product_id}
                  )
    
                UNION ALL
    
                SELECT f.follower_id, c.lvl + 1
                FROM chain c
                JOIN follows f ON f.followee_id = c.current_id
                JOIN purchases p ON f.follower_id = p.user_id
                WHERE c.lvl < {level}
                  AND p.product_id = {product_id}
            ),
            ShortestPaths AS (
                SELECT current_id, MIN(lvl) as min_lvl
                FROM chain
                GROUP BY current_id
            )
            SELECT COUNT(current_id) as viral_buyers
            FROM ShortestPaths
            WHERE min_lvl > 0 AND min_lvl <= {level};
            """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def query_4_viral_product_circle(self, product_id, level):
        if level == 0:
            query = f"""
            SELECT COUNT(DISTINCT p1.user_id) as viral_buyers
            FROM purchases p1
            WHERE p1.product_id = {product_id}
              AND NOT EXISTS (
                SELECT 1 FROM follows f
                JOIN purchases p2 ON f.followee_id = p2.user_id
                WHERE f.follower_id = p1.user_id AND p2.product_id = {product_id}
              );
            """
        else:
            query = f"""
            WITH RECURSIVE chain AS (
                SELECT p1.user_id AS current_id, 0 AS lvl
                FROM purchases p1
                WHERE p1.product_id = {product_id}
                  AND NOT EXISTS (
                    SELECT 1 FROM follows f_init
                    JOIN purchases p_init ON f_init.followee_id = p_init.user_id
                    WHERE f_init.follower_id = p1.user_id AND p_init.product_id = {product_id}
                  )
    
                UNION ALL
    
                SELECT f.follower_id, c.lvl + 1
                FROM chain c
                JOIN follows f ON f.followee_id = c.current_id
                JOIN purchases p ON f.follower_id = p.user_id
                WHERE c.lvl < {level}
                  AND p.product_id = {product_id}
            ),
            ShortestPaths AS (
                SELECT current_id, MIN(lvl) as min_lvl
                FROM chain
                GROUP BY current_id
            )
            SELECT COUNT(current_id) as viral_buyers
            FROM ShortestPaths
            WHERE min_lvl = {level};
            """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_stats(self):
        try:
            self.cursor.execute("SELECT COUNT(*) as cnt FROM users")
            users = self.cursor.fetchone()['cnt']
            self.cursor.execute("SELECT COUNT(*) as cnt FROM products")
            products = self.cursor.fetchone()['cnt']
            self.cursor.execute("SELECT COUNT(*) as cnt FROM follows")
            follows = self.cursor.fetchone()['cnt']
            self.cursor.execute("SELECT COUNT(*) as cnt FROM purchases")
            purchases = self.cursor.fetchone()['cnt']
            return {'users': users, 'products': products, 'follows': follows, 'purchases': purchases}
        except Exception:
            return None

    def close(self):
        self.cursor.close()
        self.conn.close()