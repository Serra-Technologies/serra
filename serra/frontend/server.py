from flask import Flask, render_template
from serra.config import TEMPLATE_FOLDER

def start_server():
    app = Flask(__name__, 
                template_folder=TEMPLATE_FOLDER)

    @app.route('/')
    def serve_index():
        return render_template('index.html')

    app.run()

if __name__ == '__main__':
    start_server()