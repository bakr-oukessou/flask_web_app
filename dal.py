import mysql.connector as mysql
import pandas as pd

class DataBase:
    def __init__(self):
        pass

    @staticmethod
    def create_database():
        try:
            connection = mysql.connect(host="localhost", user="root", password="Lol_lol00")
            cursor = connection.cursor()

            # Create the database if it doesn't exist
            cursor.execute("CREATE DATABASE IF NOT EXISTS db_movies")

        except mysql.Error as err:
            print(f"Error: {err}")

        finally:
            if connection.is_connected():
                connection.close()

    @staticmethod
    def create_tables():
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            # Create t_moviess table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS t_moviess (
                    MOVIES VARCHAR(255),
                    YEAR VARCHAR(10),
                    RATING VARCHAR(10),
                    VOTES INT,
                    RunTime INT
                )
            """)

            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS T_user (
                        email VARCHAR(255),
                        password VARCHAR(255),
                        isAdmin BOOLEAN
                    )
                """)

        except mysql.Error as err:
            print(f"Error: {err}")

        finally:
            if connection.is_connected():
                connection.close()

    @staticmethod
    def connect():
        return mysql.connect(
            host="localhost",
            user="root",
            password="Lol_lol00",
            database="db_movies"
        )
    

class DataETL:
    con = DataBase.connect()

    def extract(file_path):
        try:
            df = pd.read_csv(file_path,na_values=',,')
            df.dropna(inplace=True)
            return df
        except Exception as e:
            print(f"Error during extraction: {e}")
            return None
    
    def transform(df):
            try:
                columns_to_extract = ['MOVIES','YEAR','RATING','VOTES','RunTime']
                df['YEAR'] = df['YEAR'].str.extract('(\d{4})')
                df['VOTES'] = pd.to_numeric(df['VOTES'].str.replace(',', ''), errors='coerce')
                df_columns = df[columns_to_extract]
                records = df_columns.to_records(index=False)
                records_list = list(records)
                print(records_list)
                records = [tuple(row) for row in df[['MOVIES','YEAR','RATING','VOTES','RunTime']].to_records(index=False)]                
                return records_list
            except Exception as e:
                print(f"Erreur lors de la transformation des données : {e}")
                return None

    def load(data):
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            for index, row in df.iterrows():
                
                movie = row['MOVIES']
                year = row['YEAR']
                rating = row['RATING']
                votes = row['VOTES']
                runtime = row['RunTime']
           
                insert_query = cursor.execute("""
                    INSERT INTO T_movies 
                    (movies, year, rating, votes, runtime) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (movie, year, rating, votes, runtime))

            normalized_data = [(movie, str(year)[:4], rating, votes, runtime) for movie, year, rating, votes, runtime in data]

            cursor.executemany(insert_query, normalized_data)
            connection.commit()

            return True

        except mysql.Error as err:
            print(f"Error during loading: {err}")
            return False

        finally:
            if connection.is_connected():
                cursor.close()

class UserDao:
    @staticmethod
    def authenticate(email, password):
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = "SELECT * FROM T_user WHERE email = %s AND password = %s"
            cursor.execute(query, (email, password))

            user = cursor.fetchone()

            return user is not None

        except mysql.Error as err:
            print(f"Error checking user authentication: {err}")
            return False

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    @staticmethod
    def add(email, password, isAdmin=True):
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = "INSERT INTO T_user (email, password, isAdmin) VALUES (%s, %s, %s)"
            cursor.execute(query, (email, password, isAdmin))
            connection.commit()

            return True

        except mysql.Error as err:
            print(f"Error creating user: {err}")
            return False

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

class MovieDao:
    @staticmethod
    def getAll():
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = "SELECT * FROM t_movies"
            cursor.execute(query)
            movies = cursor.fetchall()

            return movies

        except mysql.Error as err:
            print(f"Error retrieving movies: {err}")
            return None

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    @staticmethod
    def update(movie_id, new_values):
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = """
                UPDATE t_movies 
                SET movies = %s, year = %s, rating = %s, votes = %s, runtime = %s 
                WHERE id = %s
            """
            cursor.execute(query, (new_values['movies'], new_values['year'], new_values['rating'], new_values['votes'], new_values['runtime'], movie_id))
            connection.commit()

            return True

        except mysql.Error as err:
            print(f"Error updating movie: {err}")
            return False

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    @staticmethod
    def delete(movie_id):
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = "DELETE FROM t_movies WHERE id = %s"
            cursor.execute(query, (movie_id,))
            connection.commit()

            return True

        except mysql.Error as err:
            print(f"Error deleting movie: {err}")
            return False

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    @staticmethod
    def countRating():
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = """
                SELECT COUNT(*), 
                    CASE 
                        WHEN rating < 5 THEN 'rating<5' 
                        WHEN rating >= 5 AND rating < 8 THEN '5<=rating<8' 
                        WHEN rating >= 8 THEN 'rating>=8' 
                    END AS rating_category 
                FROM t_movies 
                GROUP BY rating_category
            """
            cursor.execute(query)
            count_ratings = cursor.fetchall()

            return count_ratings

        except mysql.Error as err:
            print(f"Error counting ratings: {err}")
            return None

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    @staticmethod
    def rating_votes():
        try:
            connection = DataBase.connect()
            cursor = connection.cursor()

            query = "SELECT rating, SUM(votes) FROM t_movies GROUP BY rating"
            cursor.execute(query)
            rating_votes_list = cursor.fetchall()

            return rating_votes_list

        except mysql.Error as err:
            print(f"Error getting rating votes: {err}")
            return None

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

if __name__ == '__main__':
    file_path = './movies.csv'
    DataBase.create_database()
    DataBase.create_tables()

    df = DataETL.extract(file_path=file_path)

    if df is not None:
        print("Extraction terminée avec succès.")

        transformed_data = DataETL.transform(df)
        

        if transformed_data is not None:
            print(transformed_data)
            DataETL.load(transformed_data)            
        else:
            print("Erreur lors de la transformation. Vérifiez les méthodes de transformation.")
    else:
        print("Erreur lors de l'extraction. Vérifiez la méthode d'extraction.")
