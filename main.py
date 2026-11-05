from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
import os
import requests
import time
import json
from dotenv import load_dotenv

# This tells Python to find your .env file and load the passwords!
load_dotenv()

app = FastAPI()

# --- REPLACE WITH YOUR ACTUAL KEY ---
ASSEMBLYAI_API_KEY = "06e1f4b59d1a465aab5c97192d4941f9"

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://https://medical-portal-frontend-p21x.onrender.com",
        "http://localhost:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"], # <--- MAKE SURE THIS IS ["*"]
    allow_headers=["*"],
)

# ==========================================
# DATABASE CONNECTION & AUDIT LOGS
# ==========================================
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "password"),
        port=os.getenv("DB_PORT", "5432")
    )

def audit_action(transcript_id, user_id, action):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO audit_logs (transcript_id, user_id, action) VALUES (%s, %s, %s)",
        (transcript_id, user_id, action)
    )
    conn.commit()
    conn.close()

# ==========================================
# PYDANTIC MODELS
# ==========================================
class UserCreate(BaseModel):
    name: str
    email: str
    role: str
    team_id: int

class RoleUpdate(BaseModel):
    role: str

class LoginRequest(BaseModel):
    email: str

class BulkAssign(BaseModel):
    filenames: list[str]
    user_id: int | None = None
    team_id: int | None = None
    requester_id: int

class FileUpdate(BaseModel):
    filename: str
    new_status: str
    corrected_text: str
    user_id: int

class TeamCreate(BaseModel):
    name: str

class AudioIngest(BaseModel):
    filename: str
    audio_url: str
    team_id: int

# ==========================================
# ASSEMBLY AI INGESTION ENGINE (BACKGROUND WORKER)
# ==========================================
def process_transcription(filename: str, audio_url: str, team_id: int):
    """This runs silently in the background so the frontend doesn't freeze!"""
    headers = {
        "authorization": ASSEMBLYAI_API_KEY,
        "content-type": "application/json"
    }
    
    try:
        # 1. Submit the audio URL to AssemblyAI
        response = requests.post(
            "https://api.assemblyai.com/v2/transcript", 
            json={"audio_url": audio_url}, 
            headers=headers
        )
        response.raise_for_status()
        transcript_id = response.json().get("id")
        
        # 2. Poll AssemblyAI every 5 seconds until it finishes
        while True:
            poll_res = requests.get(
                f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
                headers=headers
            ).json()
            
            if poll_res["status"] == "completed":
                clean_text = poll_res["text"]
                # Optional: Grab the confidence scores for the colored magic-words UI
                word_data = json.dumps([
                    {"text": w["text"], "start": w["start"], "end": w["end"], "conf": w["confidence"]} 
                    for w in poll_res.get("words", [])
                ])
                break
            elif poll_res["status"] == "error":
                clean_text = f"ERROR: AssemblyAI failed to transcribe. Details: {poll_res.get('error')}"
                word_data = None
                break
            
            time.sleep(5) # Wait 5 seconds before asking again
            
        # 3. Save the finished transcript to PostgreSQL
        conn = get_db_connection()
        c = conn.cursor()
        
        # Insert the file directly into the Team's bucket as 'Unassigned'
        c.execute(
            """INSERT INTO transcripts (filename, text, status, team_id, word_data, audio_length, audio_url) 
               VALUES (%s, %s, 'Unassigned', %s, %s, %s, %s) RETURNING id""",
            (filename, clean_text, team_id, word_data, audio_duration, audio_url)
        )
        new_row_id = c.fetchone()[0]
        
        # FIX: Commit and close the transcript transaction FIRST
        conn.commit()
        conn.close()
        
        # NOW it is safe to log the audit action, because the transcript officially exists!
        audit_action(new_row_id, None, f"System ingested {filename} via AssemblyAI into Team {team_id}")
        
        print(f"✅ SUCCESSFULLY INGESTED: {filename}")
        
    except Exception as e:
        print(f"❌ BACKGROUND INGESTION FAILED for {filename}: {str(e)}")

@app.post("/ingest-audio")
def ingest_audio(data: AudioIngest, background_tasks: BackgroundTasks):
    """Endpoint triggered by Azure Webhooks or the Admin UI to start ingestion"""
    # Hand the heavy lifting off to the background worker
    background_tasks.add_task(process_transcription, data.filename, data.audio_url, data.team_id)
    return {"message": f"Processing started for {data.filename}. It will appear in Team {data.team_id}'s queue shortly."}


# ==========================================
# AUTHENTICATION & IDENTITY
# ==========================================
@app.post("/auth/sync")
def sync_user(req: LoginRequest):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id, name, role, team_id FROM users WHERE entra_email = %s;", (req.email,))
    row = c.fetchone()
    conn.close()
    if row: return {"exists": True, "id": row[0], "name": row[1], "role": row[2], "team_id": row[3]}
    return {"exists": False, "role": "Unauthorized"}

# ==========================================
# USER MANAGEMENT (TEAMS & USERS)
# ==========================================
@app.post("/teams")
def create_team(team: TeamCreate):
    conn = get_db_connection()
    c = conn.cursor()
    # We insert the name into both the new 'name' and legacy 'team_name' columns
    # to satisfy PostgreSQL's strict NOT NULL rules!
    c.execute(
        "INSERT INTO teams (name, team_name) VALUES (%s, %s) RETURNING id;", 
        (team.name, team.name)
    )
    conn.commit()
    conn.close()
    return {"message": "Team created"}

@app.get("/teams")
def get_teams():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id, name FROM teams ORDER BY id ASC;")
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1]} for r in rows]

@app.delete("/teams/{team_id}")
def delete_team(team_id: int):
    conn = get_db_connection()
    c = conn.cursor()
    # SAFETY FIRST: Unassign users and files from this team before deleting it!
    c.execute("UPDATE users SET team_id = NULL WHERE team_id = %s", (team_id,))
    c.execute("UPDATE transcripts SET team_id = NULL WHERE team_id = %s", (team_id,))
    
    # Now it is safe to delete the team
    c.execute("DELETE FROM teams WHERE id = %s", (team_id,))
    conn.commit()
    conn.close()
    return {"message": "Team deleted successfully"}

@app.post("/users")
def create_user(user: UserCreate):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO users (name, email, entra_email, role, team_id) VALUES (%s, %s, %s, %s, %s) RETURNING id;",
        (user.name, user.email, user.email, user.role, user.team_id)
    )
    conn.commit()
    conn.close()
    return {"message": "User created securely"}

@app.get("/users")
def get_all_users(requester_id: int = None):
    conn = get_db_connection()
    c = conn.cursor()

    if requester_id:
        c.execute("SELECT role, team_id FROM users WHERE id = %s", (requester_id,))
        user = c.fetchone()
        
        if user and user[0] == "Team Manager":
            c.execute("""
                SELECT u.id, u.name, u.entra_email, u.role, t.name 
                FROM users u 
                LEFT JOIN teams t ON u.team_id = t.id 
                WHERE u.team_id = %s 
                ORDER BY u.id ASC;
            """, (user[1],))
        else:
            c.execute("""
                SELECT u.id, u.name, u.entra_email, u.role, t.name 
                FROM users u 
                LEFT JOIN teams t ON u.team_id = t.id 
                ORDER BY u.id ASC;
            """)
    else:
        c.execute("""
            SELECT u.id, u.name, u.entra_email, u.role, t.name 
            FROM users u 
            LEFT JOIN teams t ON u.team_id = t.id 
            ORDER BY u.id ASC;
        """)
        
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "email": r[2], "role": r[3], "team_name": r[4] or "None"} for r in rows]

@app.patch("/users/{user_id}/role")
def update_user_role(user_id: int, data: RoleUpdate):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET role = %s WHERE id = %s", (data.role, user_id))
    conn.commit()
    conn.close()
    return {"message": f"Role updated to {data.role}"}

@app.delete("/users/{user_id}")
def delete_user(user_id: int):
    conn = get_db_connection()
    c = conn.cursor()
    
    # SAFETY FIRST 1: Unassign files before deleting user
    c.execute("UPDATE transcripts SET user_id = NULL, status = 'Unassigned' WHERE user_id = %s", (user_id,))
    
    # SAFETY FIRST 2: Detach their ID from the audit logs so we don't break the Foreign Key!
    # (The text of the log remains, but the strict user_id link is safely severed)
    c.execute("UPDATE audit_logs SET user_id = NULL WHERE user_id = %s", (user_id,))
    
    # NOW it is safe to delete the user
    c.execute("DELETE FROM users WHERE id = %s", (user_id,))
    
    conn.commit()
    conn.close()
    return {"message": "User deleted successfully"}

# ==========================================
# WORKFLOW & FILES
# ==========================================
@app.get("/files")
def get_files():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT filename, text, status, user_id, word_data, audio_url FROM transcripts;")
    rows = c.fetchall()
    conn.close()
    return [{"filename": r[0], "text": r[1], "status": r[2], "user_id": r[3], "word_data": r[4], "audio_url": r[5]} for r in rows]

@app.post("/update-file")
def update_file(data: FileUpdate):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id FROM transcripts WHERE filename = %s", (data.filename,))
    row = c.fetchone()
    if row:
        transcript_id = row[0]
        c.execute(
            "UPDATE transcripts SET text = %s, status = %s WHERE id = %s",
            (data.corrected_text, data.new_status, transcript_id)
        )
        audit_action(transcript_id, data.user_id, f"Saved file and updated status to {data.new_status}")
    conn.commit()
    conn.close()
    return {"message": "File updated successfully"}

# ==========================================
# ADMIN & TEAM MANAGER FILE ROUTING
# ==========================================
@app.get("/admin/files")
def get_admin_files(requester_id: int):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT role, team_id FROM users WHERE id = %s", (requester_id,))
    user = c.fetchone()
    if not user: return []

    role, team_id = user
    if role == "Admin":
        c.execute("SELECT filename, status, user_id, team_id FROM transcripts ORDER BY filename ASC;")
    elif role == "Team Manager":
        c.execute("SELECT filename, status, user_id, team_id FROM transcripts WHERE team_id = %s ORDER BY filename ASC;", (team_id,))
    else:
        return []

    rows = c.fetchall()
    conn.close()
    return [{"filename": r[0], "status": r[1], "assigned_to": r[2], "team_id": r[3]} for r in rows]

@app.post("/admin/assign")
def bulk_assign_files(data: BulkAssign):
    conn = get_db_connection()
    c = conn.cursor()
    for fname in data.filenames:
        c.execute("SELECT id FROM transcripts WHERE filename = %s", (fname,))
        row = c.fetchone()
        if row:
            transcript_id = row[0]
            if data.user_id:
                c.execute(
                    "UPDATE transcripts SET user_id = %s, status = 'Needs_Edit' WHERE id = %s", 
                    (data.user_id, transcript_id)
                )
                audit_action(transcript_id, data.requester_id, f"Assigned to user_id {data.user_id}")
            elif data.team_id:
                c.execute(
                    "UPDATE transcripts SET team_id = %s, user_id = NULL, status = 'Unassigned' WHERE id = %s", 
                    (data.team_id, transcript_id)
                )
                audit_action(transcript_id, data.requester_id, f"Routed to team_id {data.team_id}")
    conn.commit()
    conn.close()
    return {"message": f"Successfully processed {len(data.filenames)} files."}

# ==========================================
# DATABASE PATCHES (MIGRATIONS)
# ==========================================
@app.get("/patch-db")
def patch_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS name VARCHAR(255);")
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(255);")
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(255);")
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS entra_email VARCHAR(255);")
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(50);")
    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS team_id INT;")
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS user_id INT;")
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS word_data JSON;")
    conn.commit()
    conn.close()
    return {"message": "Database schema patched successfully!"}

@app.get("/patch-tier-2")
def patch_tier_2():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS team_id INT;")
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS word_data JSON;")
    conn.commit()
    conn.close()
    return {"message": "Tier-2 routing columns added!"}

@app.get("/patch-tier-3")
def patch_tier_3():
    conn = get_db_connection()
    c = conn.cursor()
    # Ensure the text column exists for AssemblyAI's output
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS text TEXT;")
    # Ensure the status column exists with a default
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'Unassigned';")
    conn.commit()
    conn.close()
    return {"message": "Final transcription columns added!"}

@app.get("/patch-tier-4")
def patch_tier_4():
    conn = get_db_connection()
    c = conn.cursor()
    # This allows the system to log automated actions without needing a human user ID
    c.execute("ALTER TABLE audit_logs ALTER COLUMN user_id DROP NOT NULL;")
    conn.commit()
    conn.close()
    return {"message": "Audit logs updated to allow automated system actions!"}

@app.get("/fix-teams")
def fix_teams():
    conn = get_db_connection()
    c = conn.cursor()
    # This finds any teams with blank names and restores a temporary name 
    # so they reappear in your React UI!
    c.execute("UPDATE teams SET name = 'Restored Team ' || id WHERE name IS NULL OR name = '';")
    conn.commit()
    conn.close()
    return {"message": "Ghost teams have been restored!"}

@app.get("/patch-audio")
def patch_audio():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS audio_url TEXT;")
    conn.commit()
    conn.close()
    return {"message": "Audio URL column added!"}

@app.get("/patch-tat")
def patch_tat():
    conn = get_db_connection()
    c = conn.cursor()
    # Add columns for TAT tracking
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS ingest_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")
    c.execute("ALTER TABLE transcripts ADD COLUMN IF NOT EXISTS audio_length INT;")
    conn.commit()
    conn.close()
    return {"message": "TAT columns added successfully!"}
