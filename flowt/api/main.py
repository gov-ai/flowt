from fastapi import FastAPI

from flowt.api.api_v1.all_routers import router as api_router_v1
from mangum import Mangum
app = FastAPI()


@app.get("/")
async def root():
    """
    Todo: Return api information
    """
    return {"message`": "Welcome to project Flowt!"}

# comment out routers here to add / remove
app.include_router(api_router_v1, prefix="/api/v1")

handler = Mangum(app)