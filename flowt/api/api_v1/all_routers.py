from fastapi import APIRouter
from .endpoints import scrape

router = APIRouter()
router.include_router(scrape.router, prefix="/scrape", tags=["Scrape Data"])