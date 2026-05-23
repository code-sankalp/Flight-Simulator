import pickle
from pathlib import Path

import pandas as pd

ARTIFACT_DIR = Path(__file__).resolve().parent / "model_artifacts"
MODEL_PATH = ARTIFACT_DIR / "model.pkl"
ENCODINGS_PATH = ARTIFACT_DIR / "encodings.pkl"


def load_artifacts(model_path=MODEL_PATH, encodings_path=ENCODINGS_PATH):
    if not model_path.exists():
        raise FileNotFoundError(f"Model artifact not found: {model_path}")
    if not encodings_path.exists():
        raise FileNotFoundError(f"Encoding artifact not found: {encodings_path}")

    with model_path.open("rb") as model_file:
        model = pickle.load(model_file)
    with encodings_path.open("rb") as encoding_file:
        encodings = pickle.load(encoding_file)
    return model, encodings


def predict_delay(origin, destination, airline, model=None, encodings=None):
    if model is None or encodings is None:
        model, encodings = load_artifacts()

    origin_code = encodings["origin"].get(origin)
    destination_code = encodings["destination"].get(destination)
    airline_code = encodings["airline"].get(airline)

    if None in [origin_code, destination_code, airline_code]:
        raise ValueError(
            "Unknown input. "
            f"Known origins: {list(encodings['origin'].keys())}; "
            f"known destinations: {list(encodings['destination'].keys())}; "
            f"known airlines: {list(encodings['airline'].keys())}"
        )

    features = pd.DataFrame(
        [[origin_code, destination_code, airline_code]],
        columns=["origin_code", "destination_code", "airline_code"],
    )
    prediction = model.predict(features)[0]

    if hasattr(model, "predict_proba"):
        probability = model.predict_proba(features)[0]
        delay_prob = float(round(probability[1] * 100, 1))
        ontime_prob = float(round(probability[0] * 100, 1))
    else:
        delay_prob = None
        ontime_prob = None

    return {
        "origin": origin,
        "destination": destination,
        "airline": airline,
        "status": "DELAYED" if prediction == 1 else "ON_TIME",
        "delay_probability": delay_prob,
        "ontime_probability": ontime_prob,
    }


def print_prediction(result):
    print(f"Flight: {result['origin']} -> {result['destination']} | Airline: {result['airline']}")
    print(f"Prediction : {result['status']}")
    if result["delay_probability"] is not None:
        print(f"Delay probability  : {result['delay_probability']}%")
        print(f"On-time probability: {result['ontime_probability']}%")
    print("-" * 50)


def main():
    print("Loading model...")
    model, encodings = load_artifacts()
    print("Model loaded successfully\n")
    print("=" * 50)
    print("FLIGHT DELAY PREDICTIONS")
    print("=" * 50 + "\n")

    examples = [
        ("DEL", "BOM", "IndiGo"),
        ("BLR", "HYD", "Air India"),
        ("CCU", "MAA", "SpiceJet"),
        ("PNQ", "DEL", "Vistara"),
        ("AMD", "BLR", "GoFirst"),
    ]
    for origin, destination, airline in examples:
        print_prediction(predict_delay(origin, destination, airline, model, encodings))


if __name__ == "__main__":
    main()
