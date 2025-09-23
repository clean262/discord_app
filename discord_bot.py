import os, re, hashlib, asyncio, datetime as dt
import discord
import aiosqlite
from discord.ext import tasks

TOKEN = os.environ["DISCORD_TOKEN"]

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)

DB_PATH = "discord_ingest.sqlite3"

# --------- キュー & ワーカー設定 ---------
QUEUE_MAXSIZE = 5000            # 背圧
BATCH_SIZE    = 200             # まとめ書き件数
BATCH_SECONDS = 1.0             # またはこの秒数でフラッシュ
write_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

db: aiosqlite.Connection | None = None
stop_event = asyncio.Event()

VER_RE = re.compile(r'\b(v(?:ersion)?[\s:_-]?\d+(?:\.\d+){0,3})\b', re.IGNORECASE)
def extract_version(text: str) -> str | None:
    m = VER_RE.search(text or "")
    return m.group(1) if m else None

def hash_user(uid: int) -> str:
    import hashlib
    return hashlib.sha256(str(uid).encode()).hexdigest()[:16]

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  guild_id   TEXT,
  channel_id TEXT,
  thread_id  TEXT,
  author_hash TEXT,
  created_at TEXT,
  edited_at  TEXT,
  reply_to   TEXT,
  content    TEXT,
  version_tag TEXT,
  url        TEXT,
  deleted    INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_messages_guild_ts ON messages(guild_id, created_at);
"""

UPSERT_SQL = """
INSERT INTO messages (
  message_id,guild_id,channel_id,thread_id,author_hash,created_at,edited_at,
  reply_to,content,version_tag,url,deleted
) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)
ON CONFLICT(message_id) DO UPDATE SET
  edited_at=excluded.edited_at,
  content=excluded.content,
  version_tag=excluded.version_tag,
  deleted=0;
"""

DELETE_SQL = "UPDATE messages SET deleted=1 WHERE message_id=?;"

async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.executescript(CREATE_SQL)
    await db.commit()

async def writer_worker():
    """キューから取り出してバッチ書き込み"""
    assert db is not None
    pending_upserts = []
    pending_deletes = []
    last_flush = asyncio.get_event_loop().time()

    while not (stop_event.is_set() and write_queue.empty()):
        timeout = max(0.0, BATCH_SECONDS - (asyncio.get_event_loop().time() - last_flush))
        try:
            item = await asyncio.wait_for(write_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            item = None

        if item:
            kind, payload = item
            if kind == "upsert":
                pending_upserts.append(payload)
            elif kind == "delete":
                pending_deletes.append((payload,))  # (message_id,)
            write_queue.task_done()

        # フラッシュ条件
        if (pending_upserts or pending_deletes) and (
            len(pending_upserts) + len(pending_deletes) >= BATCH_SIZE
            or asyncio.get_event_loop().time() - last_flush >= BATCH_SECONDS
            or stop_event.is_set()
        ):
            await db.execute("BEGIN;")
            if pending_upserts:
                await db.executemany(UPSERT_SQL, pending_upserts)
            if pending_deletes:
                await db.executemany(DELETE_SQL, pending_deletes)
            await db.commit()
            pending_upserts.clear()
            pending_deletes.clear()
            last_flush = asyncio.get_event_loop().time()

# --------- Discordイベント → キュー投入 ---------
async def enqueue_upsert(m: discord.Message):
    if m.author.bot or m.type != discord.MessageType.default:
        return
    reply_to = getattr(m.reference, "message_id", None) if m.reference else None
    payload = (
        str(m.id),
        str(getattr(m.guild, "id", "")),
        str(m.channel.id),
        str(m.channel.id) if isinstance(m.channel, discord.Thread) else None,
        hash_user(m.author.id),
        m.created_at and m.created_at.replace(tzinfo=dt.timezone.utc).isoformat(),
        m.edited_at and m.edited_at.replace(tzinfo=dt.timezone.utc).isoformat(),
        str(reply_to) if reply_to else None,
        m.content or "",
        extract_version(m.content or ""),
        m.jump_url
    )
    await write_queue.put(("upsert", payload))

async def enqueue_delete(message_id: int):
    await write_queue.put(("delete", str(message_id)))

# --------- バックフィル ---------
@tasks.loop(count=1)
async def backfill_all():
    print("[ingest] backfill start")
    for guild in client.guilds:
        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if not perms.read_messages or not perms.read_message_history:
                continue
            try:
                async for m in ch.history(limit=None, oldest_first=True):
                    await enqueue_upsert(m)
                # スレッドも
                for th in ch.threads:
                    async for m in th.history(limit=None, oldest_first=True):
                        await enqueue_upsert(m)
                async for th in ch.archived_threads(limit=None):
                    async for m in th.history(limit=None, oldest_first=True):
                        await enqueue_upsert(m)
            except discord.Forbidden:
                print(f"[skip] no permission for #{ch.name}")
            except discord.HTTPException as e:
                print(f"[warn] {e}; sleeping")
                await asyncio.sleep(2)
    print("[ingest] backfill queued")

# --------- Discord Hooks ---------
@client.event
async def on_ready():
    print("logged in as", client.user)
    await init_db()
    client.loop.create_task(writer_worker())
    backfill_all.start()

@client.event
async def on_message(message: discord.Message):
    await enqueue_upsert(message)
    if not message.author.bot and message.content == "/neko":
        await message.channel.send("にゃーん")

@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    await enqueue_upsert(after)

@client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    await enqueue_delete(payload.message_id)

@client.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent):
    for mid in payload.message_ids:
        await enqueue_delete(mid)

# --------- 終了処理（任意：Ctrl+C時に綺麗に） ---------
async def shutdown():
    stop_event.set()
    await write_queue.join()
    if db:
        await db.commit()
        await db.close()

def main():
    try:
        client.run(TOKEN)
    finally:
        # discord.pyのrunは内部ループを閉じるので、必要に応じて上位でshutdownをawait
        pass

if __name__ == "__main__":
    main()