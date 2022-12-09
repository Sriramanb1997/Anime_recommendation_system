from flask import Flask, request
from flask_cors import CORS

from recommendations import get_user_recommendations, trending


app = Flask(__name__)
CORS(app)


@app.route('/top-recommendations', methods=['GET'])
def top_recommendations():
    return trending()


@app.route('/user-recommendations', methods=['GET'])
def user_recommendations():
    user_name = request.args['user']
    return get_user_recommendations(user_name)

# flask run -p 5001
