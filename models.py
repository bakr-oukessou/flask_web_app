from dataclasses import dataclass

@dataclass
class User:
    def __init__(self, email, password, isAdmin):
        self.email = email
        self.password = password
        self.isAdmin = isAdmin

@dataclass
class Movie:
    def __init__(self, movies, year, rating, votes, runtime):
        self.movies = movies
        self.year = year
        self.rating = rating
        self.votes = votes
        self.runtime = runtime
