# 確認例 SELECT author_hash, content FROM messages ORDER BY created_at DESC LIMIT 10;
# 削除確認 SELECT author_hash, content, created_at FROM messages WHERE deleted = 1 ORDER BY created_at DESC LIMIT 10;
# Discord のメッセージURLは https://discord.com/channels/<guild_id>/<channel_id>/<message_id>らしい？
# 特定のチャンネルだけ見る SELECT author_hash, content FROM messages WHERE channel_id = '1419148902879199305' AND deleted = 0 ORDER BY created_at DESC LIMIT 10;
import os, re, hashlib, asyncio, datetime as dt
import discord
import aiosqlite
from discord.ext import tasks
import os

TOKEN = os.environ["DISCORD_TOKEN"]

intents = discord.Intents.none()
intents.guilds = True
intents.messages = True           # メッセージ関連のイベントを受け取る
intents.message_content = True    # メッセージの内容を取得する
client = discord.Client(intents=intents, max_messages=10000)

# DB_PATH = "discord_ingest.sqlite3"
DB_PATH = os.path.expanduser("~/discord_ingest.sqlite3")

# --------- キュー & ワーカー設定 ---------
QUEUE_MAXSIZE = 5000            # 背圧
BATCH_SIZE    = 1               # まとめ書き件数
BATCH_SECONDS = 0.2             # またはこの秒数でフラッシュ

db: aiosqlite.Connection | None = None
write_queue: asyncio.Queue | None = None
stop_event: asyncio.Event | None = None

VER_RE = re.compile(r'\b(v(?:ersion)?[\s:_-]?\d+(?:\.\d+){0,3})\b', re.IGNORECASE)
def extract_version(text: str) -> str | None:
    m = VER_RE.search(text or "")
    return m.group(1) if m else None

def hash_user(uid: int) -> str:
    import hashlib
    return hashlib.sha256(str(uid).encode()).hexdigest()[:16]

# messagesテーブルを作成するSQL
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  guild_id   TEXT,
  channel_id TEXT,              -- 親テキストチャンネルID（★）
  thread_id  TEXT,              -- スレッドID（非スレッドはNULL）（★）
  source_channel_id TEXT,       -- 実際に投稿された先：スレッドならthread.id、そうでなければchannel.id（★）
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

# メッセージを挿入/更新するSQL
UPSERT_SQL = """
INSERT INTO messages (
  message_id,guild_id,channel_id,thread_id,source_channel_id,author_hash,created_at,edited_at,
  reply_to,content,version_tag,url,deleted
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
ON CONFLICT(message_id) DO UPDATE SET
  edited_at=excluded.edited_at,
  content=excluded.content,
  version_tag=excluded.version_tag,
  source_channel_id=excluded.source_channel_id,  -- ★ CHANGED: 実投下先を更新
  deleted=0;
"""

DELETE_SQL = "UPDATE messages SET deleted=1 WHERE message_id=?;"

async def _ensure_column(conn: aiosqlite.Connection, table: str, column: str, decl: str):
    # ★ ADDED: 既存DB向けの軽量マイグレーション（列が無ければ追加）
    async with conn.execute(f"PRAGMA table_info({table});") as cur:
        cols = [row[1] async for row in cur]
    if column not in cols:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl};")

# データベースファイルに接続し、テーブルが存在しなければ作成
async def init_db():
    global db
    print("[db] path =", os.path.abspath(DB_PATH))
    db = await aiosqlite.connect(DB_PATH, isolation_level=None)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.executescript(CREATE_SQL)
    # ★ ADDED: 既存テーブルに source_channel_id が無ければ追加
    await _ensure_column(db, "messages", "source_channel_id", "TEXT")
    await db.commit()

writer_task = None

def _on_worker_done(t: asyncio.Task):
    try:
        t.result()
    except Exception as e:
        print("[writer][fatal]", repr(e))

# バックグラウンドで動き続けるデータベース書き込み
async def writer_worker():
    assert db is not None
    assert write_queue is not None
    assert stop_event is not None

    pending_upserts, pending_deletes = [], []
    last_flush = asyncio.get_running_loop().time()
    flushes = 0
    print("[writer] started")

    while not (stop_event.is_set() and write_queue.empty()):
        now = asyncio.get_running_loop().time()
        remaining = BATCH_SECONDS - (now - last_flush)

        try:
            if remaining > 0:
                item = await asyncio.wait_for(write_queue.get(), timeout=remaining)
            else:
                item = write_queue.get_nowait()
        except asyncio.TimeoutError:
            item = None
        except asyncio.QueueEmpty:
            item = None
        except Exception as e:
            print("[writer][get-error]", repr(e))
            await asyncio.sleep(0.01)
            continue

        if item is not None:
            kind, payload = item
            if kind == "upsert":
                pending_upserts.append(payload)
            elif kind == "delete":
                pending_deletes.append((payload,))
            elif kind == "flush":
                pass
            write_queue.task_done()

        if (pending_upserts or pending_deletes) and (
            len(pending_upserts) + len(pending_deletes) >= BATCH_SIZE
            or (asyncio.get_running_loop().time() - last_flush) >= BATCH_SECONDS
            or stop_event.is_set()
            or (item is not None and kind == "flush")
        ):
            try:
                if pending_upserts:
                    await db.executemany(UPSERT_SQL, pending_upserts)
                if pending_deletes:
                    await db.executemany(DELETE_SQL, pending_deletes)
                await db.commit()
                flushes += 1
                print(f"[writer] flush #{flushes} upserts={len(pending_upserts)} deletes={len(pending_deletes)}")
            except Exception as e:
                print("[writer][error]", repr(e))
            finally:
                pending_upserts.clear()
                pending_deletes.clear()
                last_flush = asyncio.get_running_loop().time()

        if item is None and not pending_upserts and not pending_deletes and remaining <= 0:
            await asyncio.sleep(0.005)

# --------- Discordイベント → キュー投入 ---------
def _split_channel_ids(ch: discord.abc.GuildChannel) -> tuple[str, str | None, str]:
    """★ ADDED: 親/スレッド/実投下先を計算するユーティリティ"""
    if isinstance(ch, discord.Thread):
        parent_id = str(ch.parent.id) if ch.parent else str(ch.id)
        thread_id = str(ch.id)
        source_id = str(ch.id)  # 実際に投稿された先＝スレッド
    else:
        parent_id = str(ch.id)  # 親テキストチャンネル
        thread_id = None
        source_id = str(ch.id)  # 実際に投稿された先＝このチャンネル
    return parent_id, thread_id, source_id

# discord.Messageオブジェクトを、データベースに保存しやすい形式に加工し、キューに投入
async def enqueue_upsert(m: discord.Message):
    if m.author.bot or m.type != discord.MessageType.default:
        return
    if write_queue is None:
        return
    reply_to = getattr(m.reference, "message_id", None) if m.reference else None

    # ★ CHANGED: 親/スレッド/実投下先の三分
    parent_id, thread_id, source_id = _split_channel_ids(m.channel)

    payload = (
        str(m.id),
        str(getattr(m.guild, "id", "")),
        parent_id,                    # ★ CHANGED: channel_id = 親テキストチャンネルID
        thread_id,                    # ★ CHANGED: thread_id = スレッドID or NULL
        source_id,                    # ★ ADDED : source_channel_id = 実投下先
        hash_user(m.author.id),
        m.created_at and m.created_at.replace(tzinfo=dt.timezone.utc).isoformat(),
        m.edited_at and m.edited_at.replace(tzinfo=dt.timezone.utc).isoformat(),
        str(reply_to) if reply_to else None,
        m.content or "",
        extract_version(m.content or ""),
        m.jump_url
    )
    try:
        await write_queue.put(("upsert", payload))
        print(f"[queue] put upsert {m.id} size={write_queue.qsize()}")
    except Exception as e:
        print("[queue][error]", repr(e))

async def enqueue_delete(message_id: int):
    await write_queue.put(("delete", str(message_id)))

# --------- バックフィル ---------

async def _ingest_thread(th: discord.Thread):
    try:
        if th.me is None:
            try:
                await th.join()
            except discord.Forbidden:
                pass
        async for m in th.history(limit=None, oldest_first=True):
            await enqueue_upsert(m)
    except discord.Forbidden:
        print(f"[skip] no permission for thread #{getattr(th, 'name', th.id)} ({th.id})")
    except discord.HTTPException as e:
        print(f"[warn][thread {th.id}] {e}; sleeping")
        await asyncio.sleep(2)

@tasks.loop(count=1)
async def backfill_all():
    print("[ingest] backfill start")
    for guild in client.guilds:
        # TextChannel + ForumChannel を対象（AnnouncementはTextのサブクラス）
        channels = list(guild.text_channels)
        if hasattr(guild, "forums"):
            channels.extend(guild.forums)

        for ch in channels:
            perms = ch.permissions_for(guild.me)
            if not perms.read_messages or not perms.read_message_history:
                continue
            try:
                # 親チャンネル直下の通常メッセージは TextChannel のみ
                if isinstance(ch, discord.TextChannel):
                    async for m in ch.history(limit=None, oldest_first=True):
                        await enqueue_upsert(m)

                # アクティブスレッド（チャンネル単位）
                try:
                    active = await ch.fetch_active_threads()  # 無い版あり
                    for th in active.threads:
                        await _ingest_thread(th)
                except AttributeError:
                    for th in getattr(ch, "threads", []):
                        await _ingest_thread(th)

                # 公開アーカイブ（両型でOK）
                async for th in ch.archived_threads(limit=None):
                    await _ingest_thread(th)

                # プライベートのアーカイブ（参加済みのみ）:
                # ForumChannel では古いバージョンに引数が無い→スキップ
                if isinstance(ch, discord.TextChannel):
                    try:
                        async for th in ch.archived_threads(limit=None, private=True, joined=True):
                            await _ingest_thread(th)
                    except TypeError:
                        # このdiscord.pyには private/joined 引数が無い
                        try:
                            # 一部版は private だけ受け取れる
                            async for th in ch.archived_threads(limit=None, private=True):
                                await _ingest_thread(th)
                        except TypeError:
                            pass

            except discord.Forbidden:
                print(f"[skip] no permission for #{getattr(ch, 'name', ch.id)}")
            except discord.HTTPException as e:
                print(f"[warn] {e}; sleeping")
                await asyncio.sleep(2)
    print("[ingest] backfill queued")

@client.event
async def on_ready():
    global writer_task, write_queue, stop_event
    print("logged in as", client.user)
    print("intents.message_content =", client.intents.message_content)

    write_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = asyncio.Event()

    await init_db()

    if (writer_task is None) or writer_task.done():
        writer_task = asyncio.create_task(writer_worker())
        writer_task.add_done_callback(_on_worker_done)

    if not backfill_all.is_running():
        backfill_all.start()

@client.event
async def on_message(message: discord.Message):
    print("[on_message]", message.id, "from", message.author, "len:", len(message.content or ""))
    await enqueue_upsert(message)
    if not message.author.bot and message.content == "/neko":
        await message.channel.send("にゃーん")
    if message.content.strip() == "/flush":
        await write_queue.put(("flush", None))
        await message.channel.send("flushed")

@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    await enqueue_upsert(after)

@client.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    print("[raw_edit]", payload.channel_id, payload.message_id, payload.data.keys())
    try:
        ch = client.get_channel(payload.channel_id) or await client.fetch_channel(payload.channel_id)
        msg = await ch.fetch_message(payload.message_id)
        await enqueue_upsert(msg)
    except (discord.Forbidden, discord.NotFound):
        pass

@client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    await enqueue_delete(payload.message_id)

@client.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent):
    for mid in payload.message_ids:
        await enqueue_delete(mid)

@client.event
async def on_thread_create(thread: discord.Thread):
    try:
        if thread.me is None:
            await thread.join()
    except discord.Forbidden:
        pass

async def shutdown():
    if stop_event is not None:
        stop_event.set()
    if write_queue is not None:
        await write_queue.join()
    if db:
        await db.commit()
        await db.close()

def main():
    try:
        client.run(TOKEN)
    finally:
        pass

if __name__ == "__main__":
    main()
