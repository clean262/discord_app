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
BATCH_SIZE    = 1             # まとめ書き件数
BATCH_SECONDS = 0.2             # またはこの秒数でフラッシュ

# データベースへの接続情報（コネクション）を保持するための変数
db: aiosqlite.Connection | None = None # db 変数にはaiosqlite.Connectionオブジェクトか、Noneのどちらかが入るという型ヒント
# データベースへ書き込むべきデータを一時的に溜めておくキューを保持
write_queue: asyncio.Queue | None = None
# プログラムを安全に終了させるためのイベントを管理
stop_event: asyncio.Event | None = None

# テキストからバージョン番号らしい文字列を見つけ出して抽出するための関数
VER_RE = re.compile(r'\b(v(?:ersion)?[\s:_-]?\d+(?:\.\d+){0,3})\b', re.IGNORECASE)
def extract_version(text: str) -> str | None:
    m = VER_RE.search(text or "")
    return m.group(1) if m else None

# ユーザーIDを匿名化する
# 同じハッシュ値を持つ発言は同じ人物によるもの
def hash_user(uid: int) -> str:
    import hashlib
    return hashlib.sha256(str(uid).encode()).hexdigest()[:16]

# messagesテーブルを作成するSQL
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

# メッセージを挿入/更新するSQL
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

# メッセージを削除済みにマークするSQL
DELETE_SQL = "UPDATE messages SET deleted=1 WHERE message_id=?;"

# データベースファイルに接続し、テーブルが存在しなければ作成
async def init_db():
    global db # グローバル変数 db にデータベース接続を格納
    print("[db] path =", os.path.abspath(DB_PATH))
    db = await aiosqlite.connect(DB_PATH, isolation_level=None) # 指定されたパス (DB_PATH) のデータベースファイルに非同期で接続  
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.executescript(CREATE_SQL)
    await db.commit()

writer_task = None  # ← グローバル

# 書き込みワーカーのタスクが完了（またはクラッシュ）した直後に、自動的にこの関数を呼び出す
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

    pending_upserts, pending_deletes = [], [] # メッセージを新規・更新と削除に分ける
    last_flush = asyncio.get_running_loop().time()
    flushes = 0
    print("[writer] started")

    while not (stop_event.is_set() and write_queue.empty()):
        now = asyncio.get_running_loop().time()
        remaining = BATCH_SECONDS - (now - last_flush)

        try:
            if remaining > 0:
                # 通常はブロッキング待ち
                item = await asyncio.wait_for(write_queue.get(), timeout=remaining)
            else:
                # remaining <= 0 の時は非ブロッキングで即取り出し
                item = write_queue.get_nowait()
        except asyncio.TimeoutError: # 待っている間にメッセージが来なかった場合
            item = None  
        except asyncio.QueueEmpty:
            item = None  # 今はキュー空
        except Exception as e:
            print("[writer][get-error]", repr(e))
            # 万一の暴走を抑止
            await asyncio.sleep(0.01)
            continue

        if item is not None:
            kind, payload = item
            # print(f"[writer] got item kind={kind}")  
            if kind == "upsert": # 新規・更新の場合
                pending_upserts.append(payload)
            elif kind == "delete": # 削除の場合
                pending_deletes.append((payload,))
            elif kind == "flush":
                # ここでは no-op。下の条件で即フラッシュされる
                pass
            write_queue.task_done()

        # DBへ書き込むタイミング
        if (pending_upserts or pending_deletes) and (
            len(pending_upserts) + len(pending_deletes) >= BATCH_SIZE
            or (asyncio.get_running_loop().time() - last_flush) >= BATCH_SECONDS
            or stop_event.is_set()
            or (item is not None and kind == "flush")  # ★ /flush で即時
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
            finally:  # データベースへの書き込み中にエラーが発生しても、finallyブロックは必ず実行
                pending_upserts.clear()
                pending_deletes.clear()
                last_flush = asyncio.get_running_loop().time()

        # CPU過負荷防止：何もすることが無いループでは少しだけ寝る
        if item is None and not pending_upserts and not pending_deletes and remaining <= 0:
            await asyncio.sleep(0.005)


# --------- Discordイベント → キュー投入 ---------
# discord.Messageオブジェクトを、データベースに保存しやすい形式に加工し、キューに投入
async def enqueue_upsert(m: discord.Message):
    if m.author.bot or m.type != discord.MessageType.default: # ボット自身の発言や、誰かがピン留めした等のシステムメッセージは無視
        return
    if write_queue is None:
        return  # まだ on_ready 前ならスキップ（必要なら一時バッファに）
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
    ) # オブジェクトから必要な情報を取り出し、データベースのテーブル構造に合わせたタプル（値の組）を作成
    try:
        await write_queue.put(("upsert", payload))
        print(f"[queue] put upsert {m.id} size={write_queue.qsize()}")
    except Exception as e:
        print("[queue][error]", repr(e))

# メッセージの削除があったことをキューに入れる
async def enqueue_delete(message_id: int):
    await write_queue.put(("delete", str(message_id)))

# --------- バックフィル ---------
# ボットが起動したときに過去のメッセージログを全て遡ってデータベースに登録する
@tasks.loop(count=1) # 起動後に1回だけ実行されて自動的に終了
async def backfill_all():
    print("[ingest] backfill start")
    for guild in client.guilds:
        for ch in guild.text_channels: # 全チャンネルを巡回
            perms = ch.permissions_for(guild.me) # 権限チェック
            if not perms.read_messages or not perms.read_message_history:
                continue
            try: # 履歴の全件取得 
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

@client.event
async def on_ready():
    global writer_task, write_queue, stop_event   # ★ ここが重要
    print("logged in as", client.user)
    print("intents.message_content =", client.intents.message_content)

    # Discordのイベントループ上で生成
    write_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = asyncio.Event()

    await init_db()

    if (writer_task is None) or writer_task.done():
        writer_task = asyncio.create_task(writer_worker())
        writer_task.add_done_callback(_on_worker_done)

    if not backfill_all.is_running():
        backfill_all.start()

# 新規メッセージに対するイベントハンドラ
@client.event
async def on_message(message: discord.Message):
    print("[on_message]", message.id, "from", message.author, "len:", len(message.content or ""))
    await enqueue_upsert(message)
    if not message.author.bot and message.content == "/neko":
        await message.channel.send("にゃーん")
    if message.content.strip() == "/flush":
        await write_queue.put(("flush", None))
        await message.channel.send("flushed")

# メッセージ編集(キャッシュにある場合)に対するイベントハンドラ
@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    await enqueue_upsert(after)

# メッセージ編集 (キャッシュにない場合も含む)
@client.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent):
    print("[raw_edit]", payload.channel_id, payload.message_id, payload.data.keys())
    try:
        ch = client.get_channel(payload.channel_id) or await client.fetch_channel(payload.channel_id)
        msg = await ch.fetch_message(payload.message_id)  
        await enqueue_upsert(msg)  
    except (discord.Forbidden, discord.NotFound):
        pass

# メッセージ削除
@client.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    await enqueue_delete(payload.message_id)

@client.event
async def on_raw_bulk_message_delete(payload: discord.RawBulkMessageDeleteEvent):
    for mid in payload.message_ids:
        await enqueue_delete(mid)

# スレッド作成時に自動参加
@client.event
async def on_thread_create(thread: discord.Thread):
    try:
        if thread.me is None:
            await thread.join()
    except discord.Forbidden:
        pass

# --------- 終了処理（任意：Ctrl+C時に綺麗に） ---------
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
        # discord.pyのrunは内部ループを閉じるので、必要に応じて上位でshutdownをawait
        pass

if __name__ == "__main__":
    main()