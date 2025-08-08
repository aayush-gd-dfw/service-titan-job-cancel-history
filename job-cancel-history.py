#!/usr/bin/env python3
"""
st_canceled_jobs_history.py
────────────────────────────
• Reads job_data.csv from Drive (by file name + folder ID)
• Finds all jobs where jobstatus == 'Canceled'
• Skips any job IDs already present in job-cancel-history.csv (pre-check)
• Fetches each NEW job’s cancellation log via the ServiceTitan API
• Writes the combined history to job-cancel-history.csv in the same Drive folder,
  updating the CSV after every 100 API calls and avoiding duplicates by jobId.
"""

from __future__ import annotations
import io
import os
import pickle
import sys
import logging

import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# ─── A. GOOGLE DRIVE OAUTH HELPERS ────────────────────────────────────
CLIENT_SECRET_FILE = r"client_secret_740594783744-bgsb4ditlgb5u4b7d63sosj8ku7l50ba.apps.googleusercontent.com.json"
TOKEN_PICKLE       = r"token.pkl"
SCOPES             = ["https://www.googleapis.com/auth/drive.file"]  # read/write

def drive_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE):
        with open(TOKEN_PICKLE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PICKLE, "wb") as f:
            pickle.dump(creds, f)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def find_file_id(svc, name: str, folder_id: str) -> str | None:
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    resp = svc.files().list(q=q, spaces="drive", fields="files(id)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def read_drive_csv(svc, fid: str) -> pd.DataFrame:
    buf = io.BytesIO()
    request = svc.files().get_media(fileId=fid)
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    try:
        return pd.read_csv(buf, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

def write_drive_csv(svc, df: pd.DataFrame, folder_id: str, name: str):
    """
    Write df to Drive. If a file already exists:
      • Read it
      • Combine with df
      • Drop duplicates by jobId
      • Overwrite the existing file
    """
    existing_id = find_file_id(svc, name, folder_id)

    # Merge & de-dup with existing (if any)
    if existing_id:
        try:
            existing_df = read_drive_csv(svc, existing_id)
        except Exception:
            existing_df = pd.DataFrame()
        combined = pd.concat([existing_df, df], ignore_index=True)

        # Normalize jobId to int where possible and drop dupes
        if "jobId" in combined.columns:
            combined["jobId"] = pd.to_numeric(combined["jobId"], errors="coerce")
            combined = combined.dropna(subset=["jobId"])
            combined["jobId"] = combined["jobId"].astype(int)
            combined = combined.drop_duplicates(subset=["jobId"], keep="first")
        else:
            combined = combined.drop_duplicates()

        out_df = combined
    else:
        out_df = df.copy()

    # Write the (de-duplicated) dataframe
    buf = io.BytesIO()
    out_df.to_csv(buf, index=False)
    buf.seek(0)

    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=True)
    metadata = {"name": name, "parents": [folder_id], "mimeType": "text/csv"}

    if existing_id:
        svc.files().update(fileId=existing_id, media_body=media).execute()
        print(f"🔁 Updated existing {name}")
    else:
        svc.files().create(body=metadata, media_body=media).execute()
        print(f"🆕 Created {name}")

# ─── B. DRIVE CONFIG ────────────────────────────────────────────────────
FOLDER_ID       = "1t-vJ8IV2b4ebHA1T2SCoPLUMT26JgzjM"
INPUT_FILENAME  = "job_data.csv"
OUTPUT_FILENAME = "job-cancel-history.csv"

# ─── C. SERVICE TITAN CONFIG ───────────────────────────────────────────
CLIENT_ID     = "cid.lz91bsv6oyhzq29ceb0r9g80z"
CLIENT_SECRET = "cs1.dzmosw0zu9jlhl5e0ymqkqpd04adtbc0y1am5tpugzfglcom47"
APP_KEY       = "ak1.nb1udeer5otcqp6yz34f50dq9"
TENANT_ID     = "875946535"
TOKEN_URL     = "https://auth.servicetitan.io/connect/token"

def st_token() -> str:
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_cancel_log(job_id: int, token: str) -> dict:
    url = f"https://api.servicetitan.io/jpm/v2/tenant/{TENANT_ID}/jobs/{job_id}/canceled-log"
    headers = {
        "Authorization": f"Bearer {token}",
        "ST-App-Key": APP_KEY,
        "Accept": "application/json"
    }
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code == 404:
        return {"jobId": job_id, "status": "NOT_FOUND"}
    r.raise_for_status()
    data = r.json()
    data.setdefault("jobId", job_id)
    return data

# ─── D. MAIN WORKFLOW ─────────────────────────────────────────────────
def main():
    svc = drive_service()

    # 1. locate & read job_data.csv
    job_file_id = find_file_id(svc, INPUT_FILENAME, FOLDER_ID)
    if not job_file_id:
        sys.exit(f"❌ {INPUT_FILENAME} not found in folder.")
    job_df = read_drive_csv(svc, job_file_id)

    # 2. filter canceled jobs
    if "jobstatus" not in (c.lower() for c in job_df.columns):
        sys.exit("❌ 'jobstatus' column not found in job_data.csv.")
    job_df.columns = [c.lower() for c in job_df.columns]
    canceled_ids = (
        job_df[job_df["jobstatus"] == "Canceled"]["id"]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    if not canceled_ids:
        print("✔ No canceled jobs found.")
        return

    # 2a. Pre-check existing CSV for duplicates; skip those IDs entirely
    existing_output_id = find_file_id(svc, OUTPUT_FILENAME, FOLDER_ID)
    existing_ids_set = set()
    if existing_output_id:
        try:
            existing_df = read_drive_csv(svc, existing_output_id)
            if "jobId" in existing_df.columns:
                existing_df["jobId"] = pd.to_numeric(existing_df["jobId"], errors="coerce")
                existing_ids_set = set(existing_df["jobId"].dropna().astype(int).tolist())
        except Exception as e:
            logging.warning("Could not read existing output CSV: %s", e)

    duplicates = [jid for jid in canceled_ids if jid in existing_ids_set]
    todo_ids   = [jid for jid in canceled_ids if jid not in existing_ids_set]

    if duplicates:
        print(f"ℹ️ Found {len(duplicates)} duplicate job IDs already in {OUTPUT_FILENAME}.")
    if not todo_ids:
        print("✔ All canceled job IDs are already present. Nothing new to fetch.")
        return

    print(f"🔎 Fetching cancellation history for {len(todo_ids)} jobs…")
    token = st_token()
    history_records: list[dict] = []

    for idx, jid in enumerate(todo_ids, start=1):
        try:
            rec = fetch_cancel_log(jid, token)
            history_records.append(rec)
        except Exception as e:
            logging.warning("Job %s: %s", jid, e)
            continue

        # every 100 calls: write out what we’ve fetched so far (de-duped), then refresh token
        if idx % 100 == 0:
            hist_df = pd.DataFrame(history_records)
            write_drive_csv(svc, hist_df, FOLDER_ID, OUTPUT_FILENAME)
            print(f"💾  {idx} new records written to {OUTPUT_FILENAME}")
            token = st_token()

    # final write of all NEW records (also de-dupes with any existing file)
    hist_df = pd.DataFrame(history_records)
    write_drive_csv(svc, hist_df, FOLDER_ID, OUTPUT_FILENAME)
    print(f"✅ Done – {len(history_records)} new records written to {OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()
