"""
👁️ ShadowEye — Веб Админ-Панель
=================================
FastAPI + Jinja2 + htmx
Доступна по http://localhost:8000
Защищена токеном (ADMIN_SECRET в .env)
"""

import os
import sys
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import uvicorn
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.database import Database

load_dotenv()

ADMIN_ID     = int(os.getenv("ADMIN_ID", "7965055989"))
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "shadoweye_admin_2024")
WEB_HOST     = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT     = int(os.getenv("WEB_PORT", "8000"))

db  = Database()
app = FastAPI(title="ShadowEye Admin", docs_url=None, redoc_url=None)

# Папка шаблонов
TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ── Auth ───────────────────────────────────────────────────────────────────────
def check_auth(request: Request):
    token = request.cookies.get("admin_token")
    if token != ADMIN_SECRET:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    return True


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    if secrets.compare_digest(password, ADMIN_SECRET):
        response = RedirectResponse("/", status_code=302)
        response.set_cookie("admin_token", ADMIN_SECRET, httponly=True, max_age=86400 * 7)
        return response
    return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный пароль"})


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("admin_token")
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, auth=Depends(check_auth)):
    stats = db.get_stats()
    users = db.get_all_users()
    now   = datetime.utcnow()
    # Добавляем статус к каждому юзеру
    for u in users:
        u["active"] = bool(u["expires_at"] and u["expires_at"] > now)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats":   stats,
        "users":   users,
        "now":     now,
    })


@app.post("/grant", response_class=HTMLResponse)
async def grant_sub(
    request: Request,
    user_id: int = Form(...),
    days: int    = Form(...),
    auth=Depends(check_auth)
):
    expires = db.grant_subscription(user_id, days, by_admin=ADMIN_ID)
    return RedirectResponse(f"/?msg=✅+Подписка+выдана+до+{expires.strftime('%d.%m.%Y')}", status_code=302)


@app.post("/revoke", response_class=HTMLResponse)
async def revoke_sub(
    request: Request,
    user_id: int = Form(...),
    auth=Depends(check_auth)
):
    db.revoke_subscription(user_id, by_admin=ADMIN_ID)
    return RedirectResponse("/?msg=❌+Подписка+отозвана", status_code=302)


@app.get("/user/{user_id}", response_class=HTMLResponse)
async def user_detail(request: Request, user_id: int, auth=Depends(check_auth)):
    user = db.get_user(user_id)
    if not user:
        return RedirectResponse("/?msg=Пользователь+не+найден")
    log  = db.get_subscription_log(user_id)
    now  = datetime.utcnow()
    user["active"] = bool(user["expires_at"] and user["expires_at"] > now)
    return templates.TemplateResponse("user_detail.html", {
        "request": request,
        "user":    user,
        "log":     log,
        "now":     now,
    })


# ── Запуск ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("admin_panel:app", host=WEB_HOST, port=WEB_PORT, reload=True)
