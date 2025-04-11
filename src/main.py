import logging
import os
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Настройка на логера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Определяне на базовата директория на проекта
BASE_DIR = Path(__file__).parent.parent

# Логване на пътищата за дебъг
logger.info(f"Base directory: {BASE_DIR}")
logger.info(f"Static files: {os.path.join(BASE_DIR, 'src', 'static')}")
logger.info(f"Templates: {os.path.join(BASE_DIR, 'src', 'templates')}")

# Глобална променлива за Kafka мониторинг
kafka_monitoring = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events for the FastAPI application.
    """
    pass

# Добавете това в края на файла main.py
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)