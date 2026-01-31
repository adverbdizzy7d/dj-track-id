import subprocess
from typing import List, Tuple

def _run(cmd: List[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed:\n{' '.join(cmd)}\n\nSTDERR:\n{p.stderr}")

def probe_duration_seconds(path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path
    ]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {p.stderr}")
    return float(p.stdout.strip())

def build_chunks(duration: float, chunk_sec: int, overlap_sec: int) -> List[Tuple[float, float]]:
    """
    Build (start_sec, dur_sec) chunks using a fixed window + overlap.
    Example: chunk=30, overlap=10 -> step=20 -> 0-30, 20-50, 40-70, ...
    """
    step = max(1, chunk_sec - overlap_sec)
    out: List[Tuple[float, float]] = []
    t = 0.0
    while t < duration:
        d = min(chunk_sec, max(0.0, duration - t))
        if d <= 1.0:
            break
        out.append((t, d))
        t += step
    return out

def extract_chunk_wav(src: str, dst: str, start_sec: float, dur_sec: float, sample_rate: int) -> None:
    """
    Extract a chunk into mono WAV at given sample rate.
    Shazam-like recognizers generally do better with a consistent format.
    """
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-ss", str(start_sec),
        "-t", str(dur_sec),
        "-i", src,
        "-ac", "1",
        "-ar", str(sample_rate),
        "-vn",
        dst,
        "-y"
    ]
    _run(cmd)
