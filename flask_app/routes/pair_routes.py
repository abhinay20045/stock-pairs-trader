from flask import Blueprint, jsonify

pair_bp = Blueprint("pair_bp", __name__)

@pair_bp.route("/pairs", methods=["GET"])
def get_pairs():
    sample_pairs = [
        {"pair": "AAPL-MSFT", "status": "tracking"},
        {"pair": "GOOG-AMZN", "status": "idle"}
    ]
    return jsonify(sample_pairs)