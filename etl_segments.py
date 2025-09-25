# etl_segments.py
import sqlite3, re, datetime as dt

DB = os.path.expanduser("~/discord_ingest.sqlite3")
META = os.path.expanduser("~/meta.sqlite3")  # segments/segment_mapを置くDBを分けてもOK

QUESTION_RE = re.compile(r'[?？]|どう|なぜ|できますか|教えて|わかりません')

def role_of(text: str, reply_to: str|None) -> str:
    if QUESTION_RE.search(text or ""): return "question"
    return "answer" if reply_to else "note"

def main():
    src = sqlite3.connect(DB)
    dst = sqlite3.connect(META)
    dst.executescript("""
    CREATE TABLE IF NOT EXISTS segments (
      segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
      message_id TEXT, root_message_id TEXT, channel_id TEXT, thread_id TEXT,
      created_at TEXT, version_tag TEXT, url TEXT, role TEXT, text TEXT
    );
    CREATE TABLE IF NOT EXISTS segment_map (
      message_id TEXT, segment_id INTEGER, PRIMARY KEY (message_id, segment_id)
    );
    CREATE TABLE IF NOT EXISTS etl_state (k TEXT PRIMARY KEY, v TEXT);
    """)
    cur = src.cursor()
    last = dst.execute("SELECT v FROM etl_state WHERE k='last_created_at'").fetchone()
    since = last[0] if last else "1970-01-01T00:00:00+00:00"

    rows = cur.execute("""
      SELECT message_id, channel_id, thread_id, reply_to, content, created_at, version_tag, url
      FROM messages
      WHERE deleted=0 AND created_at > ?
      ORDER BY created_at ASC
    """, (since,)).fetchall()

    for mid, ch, th, reply_to, text, ts, ver, url in rows:
        r = role_of(text, reply_to)
        root = reply_to or mid
        # 短いクリーニング
        norm = (text or "").strip()
        dst.execute("""INSERT INTO segments
          (message_id, root_message_id, channel_id, thread_id, created_at, version_tag, url, role, text)
          VALUES (?,?,?,?,?,?,?,?,?)""",
          (mid, root, ch, th, ts, ver, url, r, norm))
        seg_id = dst.execute("SELECT last_insert_rowid()").fetchone()[0]
        dst.execute("INSERT OR IGNORE INTO segment_map(message_id, segment_id) VALUES(?,?)", (mid, seg_id))

    if rows:
        dst.execute("INSERT OR REPLACE INTO etl_state(k,v) VALUES('last_created_at', ?)", (rows[-1][5],))
    dst.commit()

if __name__ == "__main__":
    main()
