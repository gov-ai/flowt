import time
from ...helpers import _get_dummy_data

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class TextRequest(BaseModel):
	text: str


dummy_idx = -1
@router.post('/scrape-dummy/')
async def post_dummy_scraped_data(req: TextRequest):
    try:
        global dummy_idx
        dummy_idx += 1
        time.sleep(1)
        sample = _get_dummy_data.get_data(dummy_idx)
        return dict(
            Date=str(sample.Date),
            Open=str(sample.Open),
            High=str(sample.High),
            Low=str(sample.Low),
            Close=str(sample.Close),
            Volume=str(sample.Volume))
    except:
        return dict(status="damn!")


@router.post('/forex/')
async def post_scraped_data(req: TextRequest):
    pair_name = req.text
    return {}