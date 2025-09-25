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
intents.messages = True            # メッセージ関連のイベントを受け取る
intents.message_content = True     # メッセージの内容を取得する
intents.reactions = True           # ★ 追加：リアクションイベントを受け取る
client = discord.Client(intents=intents, max_messages=10000)

DB_PATH = os.path.expanduser("~/discord_ingest.sqlite3")

# --------- キュー & ワーカー設定 ---------
QUEUE_MAXSIZE = 5000
BATCH_SIZE    = 200
BATCH_SECONDS = 1

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

def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

# ----------------- DB作成/マイグレーション -----------------
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  guild_id   TEXT,
  channel_id TEXT,              -- 親テキストチャンネルID
  thread_id  TEXT,              -- スレッドID（非スレッドはNULL）
  source_channel_id TEXT,       -- 実際に投稿された先（スレッドならthread.id）
  author_hash TEXT,
  created_at TEXT,
  edited_at  TEXT,
  reply_to   TEXT,
  content    TEXT,
  version_tag TEXT,
  url        TEXT,
  deleted    INTEGER DEFAULT 0
);

-- 追加インデックス（存在しなければ作成）
CREATE INDEX IF NOT EXISTS idx_messages_guild_ts   ON messages(guild_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_source_ts  ON messages(source_channel_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_reply      ON messages(reply_to);
CREATE INDEX IF NOT EXISTS idx_messages_deleted    ON messages(deleted);
CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages(channel_id, created_at);

-- リアクション集計テーブル（メッセージ×絵文字で一意）
CREATE TABLE IF NOT EXISTS reactions (
  message_id   TEXT,
  emoji_key    TEXT,    -- uni:1f44d or custom:1234567890
  emoji_name   TEXT,    -- 表示用（👍 や :name:）
  is_custom    INTEGER, -- 0:Unicode, 1:Custom
  count        INTEGER DEFAULT 0,
  last_updated TEXT,
  PRIMARY KEY (message_id, emoji_key)
);
CREATE INDEX IF NOT EXISTS idx_reactions_msg   ON reactions(message_id);
CREATE INDEX IF NOT EXISTS idx_reactions_count ON reactions(count);
"""

UPSERT_SQL = """
INSERT INTO messages (
  message_id,guild_id,channel_id,thread_id,source_channel_id,author_hash,created_at,edited_at,
  reply_to,content,version_tag,url,deleted
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)
ON CONFLICT(message_id) DO UPDATE SET
  edited_at=excluded.edited_at,
  content=excluded.content,
  version_tag=excluded.version_tag,
  source_channel_id=excluded.source_channel_id,
  deleted=0;
"""

DELETE_SQL = "UPDATE messages SET deleted=1 WHERE message_id=?;"

# ★ 追加：リアクション用SQL
UPSERT_REACTION_DELTA_SQL = """
INSERT INTO reactions (message_id, emoji_key, emoji_name, is_custom, count, last_updated)
VALUES (?,?,?,?,?,?)
ON CONFLICT(message_id, emoji_key) DO UPDATE SET
  count = MAX(0, reactions.count + excluded.count),
  emoji_name = COALESCE(excluded.emoji_name, reactions.emoji_name),
  is_custom  = COALESCE(excluded.is_custom,  reactions.is_custom),
  last_updated = excluded.last_updated;
"""

UPSERT_REACTION_SET_SQL = """
INSERT INTO reactions (message_id, emoji_key, emoji_name, is_custom, count, last_updated)
VALUES (?,?,?,?,?,?)
ON CONFLICT(message_id, emoji_key) DO UPDATE SET
  count = excluded.count,
  emoji_name = COALESCE(excluded.emoji_name, reactions.emoji_name),
  is_custom  = COALESCE(excluded.is_custom,  reactions.is_custom),
  last_updated = excluded.last_updated;
"""

DELETE_REACTIONS_FOR_MESSAGE_SQL = "DELETE FROM reactions WHERE message_id=?;"
DELETE_REACTION_EMOJI_SQL       = "DELETE FROM reactions WHERE message_id=? AND emoji_key=?;"

async def _ensure_column(conn: aiosqlite.Connection, table: str, column: str, decl: str):
    async with conn.execute(f"PRAGMA table_info({table});") as cur:
        cols = [row[1] async for row in cur]
    if column not in cols:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl};")

async def init_db():
    global db
    print("[db] path =", os.path.abspath(DB_PATH))
    db = await aiosqlite.connect(DB_PATH, isolation_level=None)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.executescript(CREATE_SQL)
    # 既存DBに列が無ければ追加
    await _ensure_column(db, "messages", "source_channel_id", "TEXT")
    await db.commit()

writer_task = None

def _on_worker_done(t: asyncio.Task):
    try:
        t.result()
    except Exception as e:
        print("[writer][fatal]", repr(e))

# ----------------- ユーティリティ -----------------
def _split_channel_ids(ch: discord.abc.GuildChannel) -> tuple[str, str | None, str]:
    if isinstance(ch, discord.Thread):
        parent_id = str(ch.parent.id) if ch.parent else str(ch.id)
        thread_id = str(ch.id)
        source_id = str(ch.id)
    else:
        parent_id = str(ch.id)
        thread_id = None
        source_id = str(ch.id)
    return parent_id, thread_id, source_id

def _emoji_key_name_iscustom(emoji) -> tuple[str, str, int]:
    """
    Returns: (emoji_key, emoji_name, is_custom)
      - unicode:  key = "uni:<codepoints-hex-joined>", name = 実文字列, is_custom = 0
      - custom :  key = "custom:<emoji_id>",          name = :name:,  is_custom = 1
    """
    # Unicode: discord.pyでは Reaction.emoji が str のことがある
    if isinstance(emoji, str):
        name = emoji
        cps = "-".join(f"{ord(ch):x}" for ch in name)
        return f"uni:{cps}", name, 0

    # Custom（Emoji / PartialEmoji）
    emoji_id = getattr(emoji, "id", None)
    emoji_name = getattr(emoji, "name", "") or ""
    if emoji_id:
        return f"custom:{emoji_id}", emoji_name, 1

    # UnicodeだがPartialEmoji/nameで来るパターン
    name = emoji_name if isinstance(emoji_name, str) else str(emoji_name)
    cps = "-".join(f"{ord(ch):x}" for ch in name)
    return f"uni:{cps}", name, 0

# ----------------- バックグラウンド書き込み -----------------
async def writer_worker():
    assert db is not None and write_queue is not None and stop_event is not None
    pending_upserts, pending_deletes = [], []
    pending_react_deltas, pending_react_sets = [], []
    pending_react_clear_msg, pending_react_clear_emoji = [], []
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
            elif kind == "reaction_delta":
                pending_react_deltas.append(payload)
            elif kind == "reaction_set":
                pending_react_sets.append(payload)
            elif kind == "reaction_clear_message":
                pending_react_clear_msg.append((payload,))
            elif kind == "reaction_clear_emoji":
                pending_react_clear_emoji.append(payload)
            elif kind == "flush":
                pass
            write_queue.task_done()

        if (pending_upserts or pending_deletes or pending_react_deltas or pending_react_sets
            or pending_react_clear_msg or pending_react_clear_emoji) and (
            len(pending_upserts) + len(pending_deletes) +
            len(pending_react_deltas) + len(pending_react_sets) +
            len(pending_react_clear_msg) + len(pending_react_clear_emoji) >= BATCH_SIZE
            or (asyncio.get_running_loop().time() - last_flush) >= BATCH_SECONDS
            or stop_event.is_set()
            or (item is not None and kind == "flush")
        ):
            try:
                if pending_upserts:
                    await db.executemany(UPSERT_SQL, pending_upserts)
                if pending_deletes:
                    await db.executemany(DELETE_SQL, pending_deletes)
                if pending_react_deltas:
                    await db.executemany(UPSERT_REACTION_DELTA_SQL, pending_react_deltas)
                if pending_react_sets:
                    await db.executemany(UPSERT_REACTION_SET_SQL, pending_react_sets)
                if pending_react_clear_msg:
                    await db.executemany(DELETE_REACTIONS_FOR_MESSAGE_SQL, pending_react_clear_msg)
                if pending_react_clear_emoji:
                    await db.executemany(DELETE_REACTION_EMOJI_SQL, pending_react_clear_emoji)
                await db.commit()
                flushes += 1
                print(f"[writer] flush #{flushes} upserts={len(pending_upserts)} deletes={len(pending_deletes)} "
                      f"react_delta={len(pending_react_deltas)} react_set={len(pending_react_sets)} "
                      f"react_clear_msg={len(pending_react_clear_msg)} react_clear_emoji={len(pending_react_clear_emoji)}")
            except Exception as e:
                print("[writer][error]", repr(e))
            finally:
                pending_upserts.clear()
                pending_deletes.clear()
                pending_react_deltas.clear()
                pending_react_sets.clear()
                pending_react_clear_msg.clear()
                pending_react_clear_emoji.clear()
                last_flush = asyncio.get_running_loop().time()

        if item is None and not pending_upserts and not pending_deletes and not pending_react_deltas and not pending_react_sets and not pending_react_clear_msg and not pending_react_clear_emoji and remaining <= 0:
            await asyncio.sleep(0.005)

# ----------------- Discordイベント → キュー投入 -----------------
async def enqueue_upsert(m: discord.Message):
    if m.author.bot or m.type != discord.MessageType.default:
        return
    if write_queue is None:
        return
    reply_to = getattr(m.reference, "message_id", None) if m.reference else None
    parent_id, thread_id, source_id = _split_channel_ids(m.channel)
    payload = (
        str(m.id),
        str(getattr(m.guild, "id", "")),
        parent_id,
        thread_id,
        source_id,
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
        # ★ この時点のリアクション集計（バックフィル/履歴取得時に効く）
        nowiso = now_utc_iso()
        for r in getattr(m, "reactions", []) or []:
            try:
                key, name, is_custom = _emoji_key_name_iscustom(r.emoji)
                count = int(getattr(r, "count", 0) or 0)
                if count > 0:
                    await write_queue.put((
                        "reaction_set",
                        (str(m.id), key, name, is_custom, count, nowiso)
                    ))
            except Exception as e:
                # 型差異や未知ケースでも落とさない
                print("[reactions][encode-error]", repr(e), "emoji=", repr(getattr(r, "emoji", None)))
    except Exception as e:
        print("[queue][error]", repr(e))




async def enqueue_delete(message_id: int):
    await write_queue.put(("delete", str(message_id)))
    # 連動してリアクションも削除（整合性維持）
    await write_queue.put(("reaction_clear_message", str(message_id)))

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
        channels = list(guild.text_channels)
        if hasattr(guild, "forums"):
            channels.extend(guild.forums)
        for ch in channels:
            perms = ch.permissions_for(guild.me)
            if not perms.read_messages or not perms.read_message_history:
                continue
            try:
                if isinstance(ch, discord.TextChannel):
                    async for m in ch.history(limit=None, oldest_first=True):
                        await enqueue_upsert(m)

                # アクティブスレッド
                try:
                    active = await ch.fetch_active_threads()
                    for th in active.threads:
                        await _ingest_thread(th)
                except AttributeError:
                    for th in getattr(ch, "threads", []):
                        await _ingest_thread(th)

                # 公開アーカイブ
                async for th in ch.archived_threads(limit=None):
                    await _ingest_thread(th)

                # プライベートアーカイブ（参加済）
                if isinstance(ch, discord.TextChannel):
                    try:
                        async for th in ch.archived_threads(limit=None, private=True, joined=True):
                            await _ingest_thread(th)
                    except TypeError:
                        try:
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

# --------- イベントハンドラ ---------
@client.event
async def on_ready():
    global writer_task, write_queue, stop_event
    print("logged in as", client.user)
    print("intents.message_content =", client.intents.message_content, "/ reactions =", client.intents.reactions)
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
    print("[raw_edit]", payload.channel_id, payload.message_id, getattr(payload, "data", {}).keys())
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

# --------- リアクションイベント ---------
@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if write_queue is None: return
    key, name, is_custom = _emoji_key_name_iscustom(payload.emoji)
    await write_queue.put(("reaction_delta", (str(payload.message_id), key, name, is_custom, +1, now_utc_iso())))

@client.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if write_queue is None: return
    key, name, is_custom = _emoji_key_name_iscustom(payload.emoji)
    await write_queue.put(("reaction_delta", (str(payload.message_id), key, name, is_custom, -1, now_utc_iso())))

@client.event
async def on_raw_reaction_clear(payload: discord.RawReactionClearEvent):
    if write_queue is None: return
    # メッセージの全リアクションが消去された
    await write_queue.put(("reaction_clear_message", str(payload.message_id)))

@client.event
async def on_raw_reaction_clear_emoji(payload: discord.RawReactionClearEmojiEvent):
    if write_queue is None: return
    key, name, is_custom = _emoji_key_name_iscustom(payload.emoji)
    await write_queue.put(("reaction_clear_emoji", (str(payload.message_id), key)))

# --------- シャットダウン ---------
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
