import asyncio
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from store import Store
from chunker import probe_duration_seconds, extract_chunk_wav, build_chunks
from shazam_recognizer import (
    ShazamIORecognizer,
    extract_track_id,
    extract_artist_title,
    extract_confidence
)
from input_resolver import resolve_audio_input

console = Console()

@dataclass
class TrackAgg:
    shazam_track_id: str
    artist: str
    title: str
    confidence_max: float
    support: int
    first_seen_sec: float
    last_seen_sec: float

def load_config(path: str) -> Dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def sha1_of_file(path: str) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def utc_run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

async def run_pipeline(
    audio_path: str,
    video_id: str,
    store: Store,
    run_dir: Path,
    chunk_seconds: int,
    overlap_seconds: int,
    sample_rate: int,
    max_parallel: int
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    work_dir = Path("work")
    work_dir.mkdir(parents=True, exist_ok=True)

    recognizer = ShazamIORecognizer()
    duration = probe_duration_seconds(audio_path)
    chunks = build_chunks(duration, chunk_seconds, overlap_seconds)

    src_hash = sha1_of_file(audio_path)
    sem = asyncio.Semaphore(max_parallel)

    tracks: Dict[str, TrackAgg] = {}

    async def process_one(idx: int, start: float, dur: float) -> None:
        chunk_key = f"{src_hash}:{start:.2f}:{dur:.2f}:{sample_rate}"

        resp = store.get_chunk(chunk_key)
        if resp is None:
            wav_path = str(work_dir / f"chunk_{idx}_{int(start)}.wav")
            extract_chunk_wav(audio_path, wav_path, start, dur, sample_rate)

            async with sem:
                resp = await recognizer.recognize_file(wav_path)

            store.put_chunk(chunk_key, resp)

        if not resp:
            return

        tid = extract_track_id(resp)
        artist, title = extract_artist_title(resp)
        if not tid or not artist or not title:
            return

        raw_conf = extract_confidence(resp)

        agg = tracks.get(tid)
        if not agg:
            base = float(raw_conf) if raw_conf is not None else 0.0
            tracks[tid] = TrackAgg(
                shazam_track_id=tid,
                artist=artist,
                title=title,
                confidence_max=base,
                support=1,
                first_seen_sec=start,
                last_seen_sec=start
            )
        else:
            agg.support += 1
            agg.first_seen_sec = min(agg.first_seen_sec, start)
            agg.last_seen_sec = max(agg.last_seen_sec, start)
            if raw_conf is not None:
                agg.confidence_max = max(agg.confidence_max, float(raw_conf))

        # Computed confidence based on how many chunks matched this track.
        # This tends to be more reliable for DJ sets than any single match score.
        agg2 = tracks[tid]
        computed = min(1.0, agg2.support / 3.0)  # 1 hit=0.33, 2 hits=0.66, 3+=1.0
        agg2.confidence_max = max(agg2.confidence_max, computed)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Analyzing chunks", total=len(chunks))

        coros = [process_one(i, s, d) for i, (s, d) in enumerate(chunks)]
        batch = 40
        for b in range(0, len(coros), batch):
            await asyncio.gather(*coros[b:b+batch])
            progress.update(task, advance=min(batch, len(coros) - b))

    rows: List[Dict[str, Any]] = []
    for tid, agg in sorted(tracks.items(), key=lambda kv: (-kv[1].confidence_max, -kv[1].support)):
        rows.append({
            "artist": agg.artist,
            "title": agg.title,
            "shazam_track_id": agg.shazam_track_id,
            "confidence": round(float(agg.confidence_max), 3),
            "support": agg.support,
            "first_seen_sec": int(agg.first_seen_sec),
            "last_seen_sec": int(agg.last_seen_sec),
            "video_id": video_id,
            "source_audio": Path(audio_path).name
        })

    # Write results
    (run_dir / "results.json").write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")

    import csv
    csv_fields = list(rows[0].keys()) if rows else [
        "artist", "title", "shazam_track_id", "confidence", "support",
        "first_seen_sec", "last_seen_sec", "video_id", "source_audio"
    ]
    with (run_dir / "results.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=csv_fields)
        w.writeheader()
        w.writerows(rows)

    # Snapshot SQLite cache into run folder
    cache_sqlite = Path(store.path)
    (run_dir / "cache.sqlite").write_bytes(cache_sqlite.read_bytes())

    meta = {
        "video_id": video_id,
        "audio_path": str(audio_path),
        "duration_sec": duration,
        "chunk_seconds": chunk_seconds,
        "overlap_seconds": overlap_seconds,
        "sample_rate": sample_rate,
        "max_parallel_chunks": max_parallel,
        "distinct_tracks": len(tracks),
        "created_utc": datetime.now(timezone.utc).isoformat()
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

def main() -> None:
    cfg = load_config("config.yaml")

    # Ensure directories exist
    Path("cache").mkdir(parents=True, exist_ok=True)
    Path(cfg["output"]["base_dir"]).mkdir(parents=True, exist_ok=True)

    # Resolve audio source
    audio_path, video_id = resolve_audio_input(cfg["input"])

    console.print(f"Using video_id: {video_id}")
    console.print(f"Using audio: {audio_path}")

    # Prepare store + run folder
    store = Store(cfg["cache"]["sqlite_path"])
    run_dir = Path(cfg["output"]["base_dir"]) / utc_run_stamp()

    # Run async pipeline
    asyncio.run(run_pipeline(
        audio_path=audio_path,
        video_id=video_id,
        store=store,
        run_dir=run_dir,
        chunk_seconds=int(cfg["audio"]["chunk_seconds"]),
        overlap_seconds=int(cfg["audio"]["overlap_seconds"]),
        sample_rate=int(cfg["audio"]["sample_rate"]),
        max_parallel=int(cfg["pipeline"]["max_parallel_chunks"]),
    ))

    console.print(f"Run completed: {run_dir}")

if __name__ == "__main__":
    main()
