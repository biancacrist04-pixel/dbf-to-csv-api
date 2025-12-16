import io
import os
import csv
from flask import Flask, request, send_file, jsonify
from dbfread import DBF

app = Flask(__name__)

def suggested_csv_name(original_name: str) -> str:
    if not original_name:
        return "output.csv"
    base = os.path.basename(original_name)
    first = base[:1].upper()
    if first == "C":
        return "CHIKUNGUNYA.csv"
    if first == "D":
        return "DENGUE.csv"
    if base.lower().endswith(".dbf"):
        return base[:-4] + ".csv"
    return base + ".csv"


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/convert")
def convert():
    if "file" not in request.files:
        return jsonify({"error": "Send multipart/form-data with field 'file'"}), 400

    f = request.files["file"]
    filename = f.filename or "input.dbf"

    dbf_bytes = f.read()
    if not dbf_bytes:
        return jsonify({"error": "Empty file"}), 400

    dbf_stream = io.BytesIO(dbf_bytes)

    table = DBF(dbf_stream, load=True, char_decode_errors="ignore")

    out = io.StringIO()
    writer = csv.writer(out)

    headers = list(table.field_names)
    writer.writerow(headers)

    for record in table:
        writer.writerow([record.get(h, "") for h in headers])

    csv_bytes = out.getvalue().encode("utf-8-sig")

    out_filename = suggested_csv_name(filename)

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=out_filename
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
