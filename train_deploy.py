import os
import pickle
import subprocess
from pathlib import Path

from google.cloud import bigquery
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
BUCKET = os.getenv("MODEL_BUCKET", "your-model-bucket")
MODEL_DIR = f"gs://{BUCKET}/models"
ARTIFACT_DIR = Path(__file__).resolve().parent / "model_artifacts"


def load_training_data():
    print("Fetching data from BigQuery...")
    bq = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            origin,
            destination,
            airline,
            CASE WHEN status = 'DELAYED' THEN 1 ELSE 0 END AS is_delayed
        FROM `{PROJECT_ID}.flight_data.flight_events`
        WHERE origin IS NOT NULL
          AND destination IS NOT NULL
          AND airline IS NOT NULL
          AND status IS NOT NULL
    """
    df = bq.query(query).to_dataframe()
    if df.empty:
        raise RuntimeError("No training rows returned from BigQuery")
    return df


def encode_features(df):
    print("\nPreparing features...")
    df = df.copy()

    encodings = {}
    for column in ["origin", "destination", "airline"]:
        category = df[column].astype("category")
        df[f"{column}_code"] = category.cat.codes
        encodings[column] = {value: code for code, value in enumerate(category.cat.categories)}

    return df, encodings


def train_model(df):
    features = ["origin_code", "destination_code", "airline_code"]
    target = "is_delayed"

    X = df[features]
    y = df[target]

    if y.nunique() < 2:
        raise RuntimeError("Training data must contain both ON_TIME and DELAYED examples")

    print("\nTraining RandomForest model...")
    stratify = y if y.value_counts().min() >= 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=stratify,
    )

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    labels = [0, 1]
    print("\nModel performance:")
    print(
        classification_report(
            y_test,
            y_pred,
            labels=labels,
            target_names=["ON_TIME", "DELAYED"],
            zero_division=0,
        )
    )
    return model


def save_artifacts(model, encodings):
    ARTIFACT_DIR.mkdir(exist_ok=True)
    model_path = ARTIFACT_DIR / "model.pkl"
    encodings_path = ARTIFACT_DIR / "encodings.pkl"

    with model_path.open("wb") as model_file:
        pickle.dump(model, model_file)
    with encodings_path.open("wb") as encodings_file:
        pickle.dump(encodings, encodings_file)

    print(f"\nModel saved to {ARTIFACT_DIR}")
    return model_path, encodings_path


def upload_artifacts(model_path, encodings_path):
    print("\nUploading model to GCS...")
    subprocess.run(["gsutil", "cp", str(model_path), f"{MODEL_DIR}/model.pkl"], check=True)
    subprocess.run(["gsutil", "cp", str(encodings_path), f"{MODEL_DIR}/encodings.pkl"], check=True)
    print(f"Model uploaded to {MODEL_DIR}")


def main():
    df = load_training_data()
    print(f"Loaded {len(df)} rows from BigQuery")
    print(df.head())

    df, encodings = encode_features(df)
    model = train_model(df)
    model_path, encodings_path = save_artifacts(model, encodings)
    upload_artifacts(model_path, encodings_path)

    print("\nDone! Model trained and saved.")
    print("Run predict.py to make predictions.")


if __name__ == "__main__":
    main()
