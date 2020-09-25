from app import app
from flask_cors import CORS
import connections_and_database_test

if __name__ == '__main__':
    connections_and_database_test.run()
    CORS(app)
    app.run(port=5000, host="0.0.0.0")
