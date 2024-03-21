from dal import UserDao, MovieDao
from models import User
from dal import DataBase

class UserService:
    def __init__(self, userDao):
        UserDao = userDao

    def signIn( email, password):
        if not email or not password:
            return "Email et mot de passe requis."

        if UserDao.authenticate(email, password):
            return True
        else:
            return "Authentification échouée."

    def signUp(email, password, isAdmin=False):
        if not email or not password:
            return "Email et mot de passe requis."

        if User(email, password, isAdmin):
            return "L'utilisateur existe déjà."

        UserDao.add(email,password)
        return True

class MovieService:
    def __init__(self, movieDao):
        MovieDao = movieDao

    def update(self, movie_id, new_values):
        return MovieDao.update(movie_id, new_values)

    def delete(self, movie_id):
        return MovieDao.delete(movie_id)

    def search(self, keyword):
        return MovieDao.search(keyword)