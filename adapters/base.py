from abc import ABC, abstractmethod


class DatabaseAdapter(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def reset_and_load(self, data):
        pass

    @abstractmethod
    def query_1_products_by_followers(self, user_id, depth):
        """Liste des produits commandés par les cercles de followers (niveau 1..n)"""
        pass

    @abstractmethod
    def query_2_specific_product_influence(self, user_id, product_id, depth):
        """Rôle d'influenceur pour un produit spécifique sur les followers"""
        pass

    @abstractmethod
    def query_3_viral_product_disk(self, product_id, level):
        """Pour un produit donné, nombre de personnes l'ayant commandé dans un disque orienté de niveau n (produits viraux)"""
        pass

    @abstractmethod
    def query_4_viral_product_circle(self, product_id, level):
        """Pour un produit donné, nombre de personnes l'ayant commandé dans un cercle orienté de niveau n (produits viraux)"""
        pass

    @abstractmethod
    def close(self):
        pass

