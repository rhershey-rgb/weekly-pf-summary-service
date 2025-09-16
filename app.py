import io, os, re, csv, threading, datetime as dt
from typing import Dict, Tuple, List

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Weekly Route Summary → CSV", version="0.1.0")

# Optional auth & limits (use X-Job-Token if you set JOB_TOKEN in env)
JOB_TOKEN = os.getenv("JOB_TOKEN", "")
MAX_BYTES = int(os.getenv("MAX_BYTES", "2000000"))  # 2 MB default
parse_lock = threading.Lock()

def require_token(headers) -> bool:
    if not JOB_TOKEN:
        return True
    return headers.get("x-job-token") == JOB_TOKEN

COLUMNS = [
    "Date", "Route No", "Total Stops", "Total Parcels", "Payment",
    "Internal Reference", "Contract Number", "Cost Centre Code",
]

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

def _two_dp(n: str) -> str:
    s = (n or "").replace("£","").replace("¬£","").strip()
    try:
        return f"{float(s):.2f}"
    except:
        return ""

def _int_or_zero(s: str) -> str:
    try:
        return str(int(str(s).strip()))
    except:
        return "0"

def _year4(y: int) -> int:
    return 2000 + y if y < 100 else y

def parse_week_ending_saturday(text: str) -> dt.date:
    m = re.search(
        r"Week\s*ending\s*Saturday\s*[:\-]?\s*(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})",
        text, re.I
    )
    if not m:
        raise ValueError("Week ending Saturday date not found")
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return dt.date(_year4(y), mth, d)

def compute_day_dates(saturday: dt.date) -> Dict[str, dt.date]:
    offsets = {"Monday": -5, "Tuesday": -4, "Wednesday": -3, "Thursday": -2, "Friday": -1, "Saturday": 0}
    return {day: saturday + dt.timedelta(days=off) for day, off in offsets.items()}

def extract_header_values(text: str) -> Dict[str, str]:
    def grab(label, pat=None, default=""):
        if pat is None:
            pat = rf"{label}\s*[:.]?\s*([A-Za-z0-9 \-_/\.]+)"
        m = re.search(pat, text, re.I)
        return (m.group(1).strip() if m else default)

    return {
        "Route No": grab("Route\s*No\.?", r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)"),
        "Invoice No": grab("Invoice\s*No"),
        "Internal Reference": grab("Internal\s*Reference"),
        "Contract Number": grab("Contract\s*Number"),
        "Cost Centre Code": grab("Cost\s*Centre\s*Code"),
    }

def extract_days_block(text: str) -> Dict[str, Tuple[str,str,str]]:
    """
    Parse lines like:
      Monday Total Stops: 107 Total Parcels: 226 Payment:281.93
    Returns {day: (stops, parcels, payment)} (strings).
    """
    out: Dict[str, Tuple[str,str,str]] = {}
    for day in DAY_NAMES:
        pat = rf"{day}\s+Total\s+Stops\s*[:\-]?\s*(\d+)\s+Total\s+Parcels\s*[:\-]?\s*(\d+)\s+Payment\s*[:\-]?\s*£?\s*([0-9]+(?:\.[0-9]{{1,2}})?)"
        m = re.search(pat, text, re.I)
        if m:
            stops, parcels, pay = m.group(1), m.group(2), m.group(3)
            out[day] = (_int_or_zero(stops), _int_or_zero(parcels), _two_dp(pay))
        else:
            out[day] = ("0", "0", _two_dp("0"))
    return out

def read_pdf_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join([(p.extract_text() or "") for p in pdf.pages])

def rows_from_pdf(pdf_bytes: bytes):
    text = read_pdf_text(pdf_bytes)
    hdr = extract_header_values(text)
    saturday = parse_week_ending_saturday(text)
    day_dates = compute_day_dates(saturday)
    day_vals = extract_days_block(text)

    for day in DAY_NAMES:
        d = day_dates[day].strftime("%Y-%m-%d")
        stops, parcels, pay = day_vals[day]
        yield {
            "Date": d,
            "Route No": hdr["Route No"],
            "Total Stops": stops,
            "Total Parcels": parcels,
            "Payment": pay,
            "Internal Reference": hdr["Internal Reference"],
            "Contract Number": hdr["Contract Number"],
            "Cost Centre Code": hdr["Cost Centre Code"],
        }

def stream_csv(pdf_bytes: bytes, download_name="weekly.csv") -> StreamingResponse:
    def gen():
        with parse_lock:
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
            w.writeheader(); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
            for row in rows_from_pdf(pdf_bytes):
                w.writerow({k: row.get(k,"") for k in COLUMNS})
                yield buf.getvalue(); buf.seek(0); buf.truncate(0)
    return StreamingResponse(gen(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename=\"{download_name}\"'})

# ---- API
class UrlIn(BaseModel):
    file_url: str

@app.get("/")
def root():
    return {"status":"ok","endpoints":["/process/url","/process/file","/healthz"]}

@app.get("/healthz")
def healthz():
    return {"status":"healthy"}

@app.post("/process/url")
def process_url(body: UrlIn, request: Request):
    if not require_token(request.headers):
        return JSONResponse(status_code=401, content={"error":"unauthorized"})
    try:
        with requests.get(body.file_url, stream=True, timeout=60) as r:
            r.raise_for_status()
            cl = r.headers.get("content-length")
            if cl and int(cl) > MAX_BYTES:
                return JSONResponse(status_code=413, content={"error":"file too large"})
            pdf_bytes = r.content
        return stream_csv(pdf_bytes, "weekly.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(request: Request, file: UploadFile = File(...)):
    if not require_token(request.headers):
        return JSONResponse(status_code=401, content={"error":"unauthorized"})
    try:
        pdf_bytes = await file.read()
        if len(pdf_bytes) > MAX_BYTES:
            return JSONResponse(status_code=413, content={"error":"file too large"})
        name = (file.filename or "weekly").replace(".pdf","") + ".csv"
        return stream_csv(pdf_bytes, name)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
