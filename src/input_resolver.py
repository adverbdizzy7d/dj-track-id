import os
from pathlib import Path
import subprocess
from typing import Tuple

def resolve_video_id() -> str:
    """
    Determine the video ID from GitHub context.

    Priority:
    1) Branch name (if not main/master)
    2) Repo name
    """
    ref = os.getenv("GITHUB_REF_NAME", "").strip()
    if ref and ref not in ("main", "master"):
        return ref

    repo = os.getenv("GITHUB_REPOSITORY", "").strip()  # owner/repo
    if repo and "/" in repo:
        return repo.split("/")[-1]

    # Local fallback
    return "local"

def _run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed:\n{' '.join(cmd)}\n\nSTDERR:\n{p.stderr}")
    return p.stdout

def ensure_audio_from_youtube(video_id: str, out_mp3: Path, youtube_cfg: dict) -> None:
    """
    Download audio from YouTube using the video ID.
    Output file is a fixed path (input/source.mp3) for reproducibility.
    """
    out_mp3.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://www.youtube.com/watch?v={video_id}"

    cmd = [
        "yt-dlp",
        "-x",
        "--audio-format", str(youtube_cfg.get("audio_format", "mp3")),
        "--audio-quality", str(youtube_cfg.get("audio_quality", 0)),
        "-o", str(out_mp3),
        url
    ]

    # If you ever want cookies support later:
    # if youtube_cfg.get("cookies"):
    #     cmd += ["--cookies", "cookies.txt"]

    _run(cmd)

def resolve_audio_input(input_cfg: dict) -> Tuple[str, str]:
    """
    Input resolution logic:
    - If input/source.mp3 exists and prefer_mp3 is true -> use it.
    - Else -> derive video ID (repo/branch) and download audio with yt-dlp.

    Returns:
      (audio_path, video_id)
    """
    mp3_path = Path(input_cfg.get("mp3_path", "input/source.mp3"))
    prefer_mp3 = bool(input_cfg.get("prefer_mp3", True))
    youtube_cfg = input_cfg.get("youtube", {})
    youtube_enabled = bool(youtube_cfg.get("enabled", True))

    video_id = resolve_video_id()

    if prefer_mp3 and mp3_path.exists():
        return str(mp3_path), video_id

    if not youtube_enabled:
        raise RuntimeError("No MP3 present and YouTube input is disabled in config.yaml")

    ensure_audio_from_youtube(video_id=video_id, out_mp3=mp3_path, youtube_cfg=youtube_cfg)

    if not mp3_path.exists():
        raise RuntimeError("yt-dlp finished but the output MP3 was not found.")

    return str(mp3_path), video_id
