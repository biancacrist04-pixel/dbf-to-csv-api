import io
import os
from flask import Flask, request, send_file, jsonify
from dbfread import DBF
import pandas as pd

app = Flask(__name__)

def dbf_bytes_to_csv_bytes(dbf_bytes: bytes) -> bytes:
    # DBFread precisa de "arquivo". Vamos usar um arquivo temporário em memória
    # (dbfread aceita file-like? geralmente é caminho; então usamos temp file)
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".dbf", delete=True) as tmp:
        tmp.write(dbf_bytes)
        tmp.flush()

        try:
            table = DBF(tmp.name, load=True, encoding="cp1252", char_decode_errors="ignore")
        except Exception:
            table = DBF(tmp.name, load=True, encoding="latin1", char_decode_errors="ignore")

        df = pd.DataFrame(iter(table))
        df.columns = [str(c).strip() for c in df.columns]

    out = io.StringIO()
    df.to_csv(out, index=False)
    return out.getvalue().encode("utf-8-sig")

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/convert")
def convert():
    if "file" not in request.files:
        return jsonify({"error": "Send multipart/form-data with field 'file'"}), 400

    f = request.files["file"]
    dbf_bytes = f.read()

    csv_bytes = dbf_bytes_to_csv_bytes(dbf_bytes)

    # devolve como arquivo CSV
    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name="output.csv",
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
