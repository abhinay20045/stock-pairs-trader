from flask import Flask
from routes.pair_routes import pair_bp

app = Flask(__name__)
app.register_blueprint(pair_bp)

@app.route('/health')
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")