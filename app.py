
import asyncio
from contextlib import asynccontextmanager
from typing import List, Optional

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import engine, log_bus, log


class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


def _on_log(entry: dict):
    try:
        loop = asyncio.get_running_loop()
        asyncio.run_coroutine_threadsafe(manager.broadcast({"type": "log", "data": entry}), loop)
    except RuntimeError:
        pass
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_bus.subscribe(_on_log)
    yield
    log_bus.unsubscribe(_on_log)
    engine.stop()
    engine.clear_accounts()


app = FastAPI(title="KINGONEWISH Quest Engine", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


class TokensIn(BaseModel):
    tokens: List[str]


class WebhookIn(BaseModel):
    url: str



@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse("static/index.html")


@app.post("/api/tokens")
async def add_tokens(body: TokensIn):
    if not body.tokens:
        raise HTTPException(400, "No tokens provided")
    result = engine.add_tokens(body.tokens)
    return result


@app.get("/api/accounts")
async def list_accounts():
    return {"accounts": engine.list_accounts()}


@app.delete("/api/accounts")
async def clear_accounts():
    was_running = engine.running
    if was_running:
        engine.stop()
    engine.clear_accounts()
    return {"ok": True}


@app.post("/api/webhook")
async def set_webhook(body: WebhookIn):
    engine.set_webhook(body.url)
    return {"ok": True, "url": body.url[:40] + "..." if len(body.url) > 40 else body.url}


@app.post("/api/start")
async def start_engine():
    ok = engine.start()
    if not ok:
        raise HTTPException(400, "Cannot start — no accounts or already running")
    return {"ok": True, "status": engine.status()}


@app.post("/api/stop")
async def stop_engine():
    engine.stop()
    return {"ok": True, "status": engine.status()}


@app.get("/api/status")
async def get_status():
    return engine.status()


@app.get("/api/logs")
async def get_logs(limit: int = 100):
    hist = log_bus.history[-limit:]
    return {"logs": hist}


@app.get("/api/claimed")
async def get_claimed():
    return {"claimed": log_bus.claimed_rewards}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    
    for entry in log_bus.history[-50:]:
        try:
            await ws.send_json({"type": "log", "data": entry})
        except Exception:
            break
    try:
        while True:
            
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
