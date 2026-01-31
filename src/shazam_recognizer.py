from typing import Optional, Dict, Any
from shazamio import Shazam

def extract_track_id(resp: Dict[str, Any]) -> Optional[str]:
    """
    Reverse-engineered Shazam responses commonly include a stable per-track key.
    Field names are not guaranteed, so we try a few candidates.
    """
    track = resp.get("track") if isinstance(resp, dict) else None
    if not track:
        return None

    for k in ["key", "id", "shazam_id", "shazamID"]:
        v = track.get(k)
        if v:
            return str(v)
    return None

def extract_artist_title(resp: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """
    Typical Shazam response uses:
    - title: track title
    - subtitle: artist
    """
    track = resp.get("track") if isinstance(resp, dict) else None
    if not track:
        return None, None

    title = track.get("title")
    artist = track.get("subtitle") or track.get("artist")
    return artist, title

def extract_confidence(resp: Dict[str, Any]) -> Optional[float]:
    """
    ShazamIO / reverse-engineered outputs do not guarantee a confidence score.
    If present, we return it as a float.
    If it's 0..100, we normalize to 0..1.
    """
    track = resp.get("track") if isinstance(resp, dict) else None
    if not track:
        return None

    for k in ["confidence", "score", "probability"]:
        v = track.get(k)
        if isinstance(v, (int, float)):
            if 1.0 < v <= 100.0:
                return float(v) / 100.0
            return float(v)
    return None

class ShazamIORecognizer:
    def __init__(self) -> None:
        self._shazam = Shazam()

    async def recognize_file(self, wav_path: str) -> Optional[Dict[str, Any]]:
        """
        Returns raw response dict if a track match exists; otherwise None.
        """
        try:
            resp = await self._shazam.recognize(wav_path)
            if not resp or not isinstance(resp, dict) or "track" not in resp:
                return None
            return resp
        except Exception:
            return None
