from flask import Flask, request, jsonify
import requests
import json
from datetime import datetime
import os

app = Flask(__name__)

JETADMIN_BASE_URL = "https://data.jetadmin.app/projects/fuss/prod/firebase_osx9/models/places/"
JETADMIN_AUTH_HEADER = {
    "Authorization": os.environ.get("JETADMIN_TOKEN")
}
WEBHOOK_URL = "https://hook.eu1.make.com/co259oby9byycbxnbv5h0j4iqpljiq98"

def parse_if_json(value):
    try:
        return json.loads(value) if isinstance(value, str) else value
    except Exception:
        return value

def normalize_url(url):
    return url.lower().replace("http://", "").replace("https://", "").replace("www.", "").rstrip("/") if url else ""

def normalize_str(s):
    return s.strip().lower() if isinstance(s, str) else s

def normalize_phone(phone):
    return ''.join(filter(str.isdigit, phone)) if phone else ""

def normalize_coords(coords):
    return {
        "latitude": round(coords.get("latitude", 0), 5),
        "longitude": round(coords.get("longitude", 0), 5)
    } if coords else {}

def sort_list(data):
    return sorted(data) if isinstance(data, list) else data

def compare_values(val1, val2, field):
    if field in ["budgetTagValues", "filterValues", "placeTypes"]:
        return sort_list(parse_if_json(val1)) == sort_list(parse_if_json(val2))
    elif field in ["isPaid", "isPromoted", "isVisible"]:
        return bool(val1) == bool(val2)
    elif field == "coordinates":
        return normalize_coords(val1) == normalize_coords(parse_if_json(val2))
    elif field == "phone":
        return normalize_phone(val1) == normalize_phone(val2)
    elif field in ["websiteURL", "facebookURL", "instagramURL", "menuURL"]:
        return normalize_url(val1) == normalize_url(val2)
    elif field in ["name", "description", "workingHours"]:
        val1_en = val1.get("en") if isinstance(val1, dict) else None
        val2_en = parse_if_json(val2).get("en") if isinstance(parse_if_json(val2), dict) else None
        return normalize_str(val1_en) == normalize_str(val2_en)
    elif field == "ratingAggregators":
        return json.dumps(val1, sort_keys=True) == json.dumps(parse_if_json(val2), sort_keys=True)
    elif field == "google_place_id":
        return normalize_str(val1) == normalize_str(val2)
    elif field in ["promo", "promocode"]:
        return normalize_str(val1 or "") == normalize_str(val2 or "")
    elif field in ["earnBonuses"]:
        return round(float(val1), 2) == round(float(val2), 2)
    elif field in ["address", "cityRef", "countryRef"]:
        return normalize_str(val1) == normalize_str(val2)
    elif field == "bonuses":
        return val1 == parse_if_json(val2)
    else:
        return val1 == val2

FIELDS_TO_COMPARE = [
    "address", "budgetTagValues", "cityRef", "countryRef", "filterValues",
    "isPaid", "isPromoted", "isVisible", "name", "phone", "placeTypes",
    "websiteURL", "workingHours", "facebookURL", "instagramURL", "menuURL",
    "coordinates", "promo", "promocode", "description", "ratingAggregators",
    "google_place_id", "bonuses", "earnBonuses"
]

@app.route("/check", methods=["POST"])
def check():
    data = request.get_json()
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"🔵 Получено {len(data)} записей")

    matched = []
    mismatched = []

    for idx, item in enumerate(data):
        document_id = item.get("document_id")
        key = item.get("key")

        if not document_id or not key:
            print(f"⚠️ Пропущен элемент #{idx}: нет document_id или key")
            continue

        print(f"\n📄 Проверяем: key={key}, document_id={document_id}")

        r = requests.get(JETADMIN_BASE_URL + document_id, headers=JETADMIN_AUTH_HEADER)
        if r.status_code != 200:
            print(f"🔴 JetAdmin вернул статус {r.status_code} для key={key}, document_id={document_id}")
            not_found.append({"key": key, "status": "request failed", "document_id": document_id})
            continue

        jet_data = r.json()
        if not jet_data:
            print(f"⚪️ Не найдено в JetAdmin: key={key}, document_id={document_id}")
            not_found.append({"key": key, "status": "place not found", "document_id": document_id})
            continue

        differences_found = False
        updated_entry = {"key": key, "date": today_str}

        for field in FIELDS_TO_COMPARE:
            value1 = item.get(field)
            value2 = jet_data.get(field)

            if not compare_values(value1, value2, field):
                differences_found = True
                updated_entry[field] = parse_if_json(value2)
                print(f"⚠️ Отличие в поле '{field}':\n  ▶️ local={value1}\n  ▶️ remote={value2}")

        if differences_found:
            mismatched.append(updated_entry)
        else:
            matched.append({"key": key, "date": today_str})
            print("✅ Запись совпадает")

    payload = {
    "matched": matched,
    "mismatched": mismatched,
    "not_found": not_found
    }
    requests.post(WEBHOOK_URL, json=payload)

    print(f"\n📤 Отправка на вебхук: matched={len(matched)}, mismatched={len(mismatched)}")
    requests.post(WEBHOOK_URL, json=payload)

    return jsonify({
    "status": "ok",
    "matched": len(matched),
    "mismatched": len(mismatched),
    "not_found": len(not_found)
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)
