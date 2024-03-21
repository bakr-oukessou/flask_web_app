import base64
from io import BytesIO
from flask import Flask, redirect, render_template, request, jsonify, session, url_for
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from services import UserService, MovieService
from dal import UserDao, MovieDao
from models import User, Movie

app = Flask(__name__)
app.secret_key = "your_secret_key"  

user_service = UserService(UserDao())
movie_service = MovieService(MovieDao())


# Routes
@app.route('/')
def home():
    return render_template('login.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # Logique d'authentification ici
        email = request.form.get('email')
        password = request.form.get('password')

        result = UserService.signIn(email, password)

        if result is True:
            session['email'] = email
            return redirect('/app')

        return render_template('login.html', error=result)

    return render_template('login.html', error=None)


@app.route('/logout')
def logout():
    session.pop('email', None)
    return redirect('/')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        signup_result = UserService.signUp(email, password)

        if signup_result is True:
            return redirect('/login')
        else:
            return render_template('register.html', error=signup_result)

    return render_template('register.html')

@app.route('/app')
def app_page():
    if 'email' in session:
        movies = MovieDao.getAll()
        # coffees = Coffee.listAllCoffee()
        # consumptions = Coffee.listAllConsumption()
        return render_template('index.html', movies=movies)
    return redirect(url_for('login.html'))




@app.route('/movies', methods=['PUT'])
def update_movie(movie_id):
    try:
        data = request.get_json()
        result = movie_service.update(movie_id, data)

        if result:
            return jsonify({"message": "Movie updated successfully"}), 200
        else:
            return jsonify({"message": "Failed to update movie"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/movies/delete', methods=['DELETE'])
def delete_movie(movie_id):
    try:
        result = movie_service.delete(movie_id)

        if result:
            return jsonify({"message": "Movie deleted successfully"}), 200
        else:
            return jsonify({"message": "Failed to delete movie"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/movies/search', methods=['GET'])
def search_movies():
    try:
        keyword = request.args.get('keyword')
        result = movie_service.search(keyword)

        if result:
            return jsonify(result), 200
        else:
            return jsonify({"message": "No movies found"}), 404

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/dashboard')
def dashboard2():
    total_consumption_data = MovieDao.countRating()

    years = [row[0] for row in total_consumption_data]
    total_consumption = [row[1] for row in total_consumption_data]

    fig, axs = plt.subplots(2,2,  figsize=(10, 8))
    
    # bar chart
    axs[0, 0].bar(years, total_consumption, color='skyblue')
    axs[0, 0].set_xlabel('Year')
    axs[0, 0].set_ylabel('Total Consumption')
    axs[0, 0].set_title('Total Consumption by Year')

    # Line plot
    axs[0, 1].plot(years, total_consumption, marker='o', color='green')
    axs[0, 1].set_xlabel('Year')
    axs[0, 1].set_ylabel('Total Consumption')
    axs[0, 1].set_title('Total Consumption Trend')



  # Save the figure to a BytesIO object
    image_stream = BytesIO()
    canvas = FigureCanvas(fig)
    canvas.print_png(image_stream)
    image_stream.seek(0)
    plot_base64 = base64.b64encode(image_stream.read()).decode('utf-8')

    # Close the figure to free up resources
    plt.close(fig)

    # Pass the file path to the template
    return render_template('dashboard.html', plot_base64=plot_base64)


# Run the application
if __name__ == '__main__':
    app.run(debug=True)
