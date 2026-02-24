# MariaDB vs Neo4j – Comparaison

## Lancement

```bash
docker-compose up -d
pip install -r requirements.txt
python main.py
```

## Menu

1. Charger un dataset
2. Exécuter une requête
3. Activer/Désactiver une base
4. Quitter

## Datasets

* Données existantes
* dataset.json
* Génération synthétique (paramétrable)

## Requêtes

1. Produits achetés par le réseau (user_id, profondeur)
2. Influence sur un produit (user_id, product_id, profondeur)
3. Viralité produit – disque orienté (product_id, niveau)
4. Viralité produit – cercle orienté (product_id, niveau)

Affichage automatique des temps d’exécution MariaDB / Neo4j.
