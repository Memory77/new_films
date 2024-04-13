from itemadapter import ItemAdapter
import re
from scrapy.exceptions import DropItem
from datetime import datetime
import os

class NewFilmsPipeline:
    def process_item(self, item, spider):

        for key in list(item.keys()):
        # Vérifier si la valeur associée à la clé est un tiret
            if item[key] == '-' or item[key] == []:
                # Remplacer par None si c'est le cas
                item[key] = None

        item['date_sortie'] = self.convert_date(item.get('date_sortie', ''))
        

        # Nettoyer et convertir la durée en minutes
        item['duree'] = self.clean_duration(item.get('duree', ''))

        # Convertir les entrées en un entier, ne rien faire si 'entrees' n'existe pas
        if 'entrees' in item:
            item['entrees'] = self.convert_entrees(item['entrees'])

        if 'budget' in item:
            item['budget'] = self.convert_entrees(item['budget'])

        # Nettoyer la description
        item['description'] = self.clean_text(item.get('description', ''))

        
        # Nettoyer le champ pays
        item['pays'] = item.get('pays', '').strip()
        
        if item['studio']:
            item['studio'] = item.get('studio', '').strip()
        

        #nettoyer anecdotes
        if item['anecdotes']:
            item['anecdotes'] = item.get('anecdotes', '').strip()
            premiere_lettre = item['anecdotes'][0]
            item['anecdotes'] = int(premiere_lettre)

        #nettoyer budget
        # Extraire uniquement le nombre de séances, s'il existe
        if 'salles' in item:
            item['salles'] = self.extract_sessions(item['salles'])
        #nettoyer le champ realisateur 
        if item['realisateur']:
            premiere_valeur = item['realisateur'][0]
            if premiere_valeur == 'De':
                item['realisateur'].pop(0)
        #nettoyer le champ acteur
        if item['acteurs']:
            premiere_valeur = item['acteurs'][0]
            if premiere_valeur == 'Avec':
                item['acteurs'].pop(0)
        
        return item
    
   
    def clean_duration(self, duration_html):
        # Extrait la durée en minutes à partir du HTML ou retourne None si non trouvable
        if duration_html:
            match = re.search(r'(\d+)h\s*(\d+)min', duration_html)
            if match:
                hours, minutes = match.groups()
                return int(hours) * 60 + int(minutes)
        return None

    def convert_entrees(self, entrees):
        # Convertit les entrées en int, gère si 'entrees' est déjà un int ou None
        if entrees is None:
            return None
        if isinstance(entrees, int):
            return entrees
        if isinstance(entrees, str):
            return int(re.sub(r'\D', '', entrees))
        return None

    def clean_text(self, text):
        # Supprime les espaces superflus dans un texte
        return re.sub(r'\s+', ' ', text).strip()

   
    def extract_sessions(self, salles):
        # Si 'salles' est déjà un entier, rien à faire
        if isinstance(salles, int):
            return salles
        # Si 'salles' est une chaîne, essayer d'extraire le nombre
        elif isinstance(salles, str):
            match = re.search(r'\d+', salles)
            if match:
                return int(match.group(0))  # Convertir le nombre trouvé en entier
        # Si aucun des cas précédents, retourner None ou une valeur par défaut
        return None

 
    def convert_date(self, date_str):
        # Conversion de la date du format '3 avril 2024' au format ISO '2024-04-03'
        try:
            return datetime.strptime(date_str, '%d %B %Y').strftime('%Y-%m-%d')
        except ValueError:
            # Tentez une conversion en supposant le français pour le nom du mois
            french_to_english = {
                'janvier': 'January', 'février': 'February', 'mars': 'March', 'avril': 'April',
                'mai': 'May', 'juin': 'June', 'juillet': 'July', 'août': 'August',
                'septembre': 'September', 'octobre': 'October', 'novembre': 'November', 'décembre': 'December'
            }
            for fr, en in french_to_english.items():
                if fr in date_str:
                    date_str = date_str.replace(fr, en)
                    break
            try:
                return datetime.strptime(date_str, '%d %B %Y').strftime('%Y-%m-%d')
            except ValueError:
                # Si la conversion échoue, retourner la chaîne originale ou une valeur par défaut
                return date_str
            
    
import mysql.connector
from mysql.connector import Error as MySQLError

class MySQLStorePipeline(object):
    def open_spider(self, spider):
        try:
            self.conn = mysql.connector.connect(
                user='Spies', 
                password='Simplon1948', 
                host='dbcinapps.mysql.database.azure.com', 
                database='dbcinapps'
            )
            self.cursor = self.conn.cursor()
        except MySQLError as e:
            spider.logger.error(f"Erreur de connexion à la base de données : {e}")
            raise

    def close_spider(self, spider):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            try:
                self.conn.close()
            except MySQLError as e:
                spider.logger.error(f"Erreur lors de la fermeture de la connexion à la base de données : {e}")

    def process_item(self, item, spider):
        insert_query = """
        INSERT INTO Films (titre, duree, salles, genre, date_sortie, pays, studio, description, image, budget, entrees, anecdotes) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        duree = VALUES(duree),
        salles = VALUES(salles),
        genre = VALUES(genre),
        date_sortie = VALUES(date_sortie),
        pays = VALUES(pays),
        studio = VALUES(studio),
        description = VALUES(description),
        image = VALUES(image),
        budget = VALUES(budget),
        entrees = VALUES(entrees),
        anecdotes = VALUES(anecdotes);
        """
        try:
            self.cursor.execute(insert_query, (
                item.get('titre'), item.get('duree'), item.get('salles'), item.get('genre'),
                item.get('date_sortie'), item.get('pays'), item.get('studio'), item.get('description'),
                item.get('image'), item.get('budget'), item.get('entrees'), item.get('anecdotes')
            ))
            self.conn.commit()
        except MySQLError as e:
            spider.logger.error(f"Erreur lors de l'insertion ou de la mise à jour des données : {e}")
            return item

        