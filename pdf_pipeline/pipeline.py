# -*- coding: utf-8 -*-
"""
pdf_pipeline.pipeline — TEXT ONLY PIPELINE (robust normalization, prefer local path)

依赖：
  - download.py  提供: async download_pdfs(...)
      * 目录参数以位置参数（第2个）传入，避免关键字不匹配
      * 并发参数名自动探测：max_download_concurrency / max_concurrency / concurrency / limit / max_workers
      * 返回可为 dict / tuple / list / str / Path
  - parser.py    提供: parse_pdfs(pdf_paths, max_parse_concurrency)

流程：
  1) 从 --urls 文件读取 URL 列表
  2) 并发下载到 --download-dir
  3) 并发抽取文本（parser.parse_pdfs 内部多线程）
  4) 每个 PDF 写成 outputs/{doc_id}.json
"""
from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .download import download_pdfs
from .parser import parse_pdfs

logger = logging.getLogger(__name__)


# ---------------------------
# 工具
# ---------------------------
def _read_urls(path: Path) -> List[str]:
    text = path.read_text(encoding="utf-8")
    urls: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # 简易行内注释
        if "#" in line:
            pos = line.find("#")
            if pos == 0:
                continue
            line = line[:pos].strip()
            if not line:
                continue
        urls.append(line)
    return urls


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    _ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _is_url(s: str) -> bool:
    s2 = s.strip().lower()
    return s2.startswith("http://") or s2.startswith("https://")


def _is_local_path(s: str) -> bool:
    """严格判定：必须不是 URL，且看起来像文件路径（包含分隔符或 .pdf 结尾）"""
    if _is_url(s):
        return False
    s2 = s.strip()
    return ("/" in s2 or "\\" in s2) or s2.lower().endswith(".pdf")


def _find_first_str(items: Iterable[Any], pred) -> Optional[str]:
    for x in items:
        if isinstance(x, str) and pred(x):
            return x
    return None


def _find_first_path_obj(items: Iterable[Any]) -> Optional[str]:
    for x in items:
        if isinstance(x, Path):
            return str(x)
    return None


def _find_first_bool(items: Iterable[Any]) -> Optional[bool]:
    for x in items:
        if isinstance(x, bool):
            return x
    return None


def _find_first_error_text(items: Iterable[Any]) -> Optional[str]:
    for x in items:
        if isinstance(x, BaseException):
            return f"{type(x).__name__}: {x}"
        if isinstance(x, str) and ("error" in x.lower() or "exception" in x.lower()):
            return x
    return None


def _normalize_one_download_result(x: Any) -> Dict[str, Any]:
    """
    归一化 download_pdfs 返回的单项为：
      { "ok": bool, "path": str|None, "url": str|None, "error": str|None }

    规则要点：
    - **优先本地路径**（Path 或非 URL 的字符串）；绝不把 URL 当成 path。
    - 兼容 dict 的多种键名：path / filepath / local_path。
    - tuple/list 中按：本地路径 > URL > ok 布尔 > 错误 文本 的优先顺序取值。
    """
    # dict 形态
    if isinstance(x, dict):
        # 兼容键名
        path = x.get("path") or x.get("filepath") or x.get("local_path")
        if isinstance(path, Path):
            path = str(path)
        if isinstance(path, str) and _is_url(path):
            # 防误：有人把 URL 塞到 path 字段
            path = None

        url = x.get("url")
        if isinstance(url, Path):
            url = str(url)
        if isinstance(url, str) and not _is_url(url):
            # 防误：有人把本地路径塞到 url 字段
            if _is_local_path(url):
                # 纠偏：把它当作 path
                path = url
                url = None

        # ok 默认为：有本地 path 即 True
        ok = bool(x.get("ok", True if path else False))

        err = x.get("error")
        if isinstance(err, BaseException):
            err = f"{type(err).__name__}: {err}"
        return {"ok": ok, "path": path, "url": url, "error": err}

    # tuple / list 形态
    if isinstance(x, (list, tuple)):
        # 先找 Path 对象（最可靠）
        path = _find_first_path_obj(x)
        # 再找字符串中的本地路径（非 URL）
        if path is None:
            path = _find_first_str(x, _is_local_path)
        # 再找 URL
        url = _find_first_str(x, _is_url)
        # 再找 ok 布尔
        ok = _find_first_bool(x)
        if ok is None:
            ok = bool(path and Path(path).exists())
        # 错误
        err = _find_first_error_text(x)
        return {"ok": bool(ok), "path": path, "url": url, "error": err}

    # 字符串：可能是路径，也可能是 URL
    if isinstance(x, str):
        if _is_local_path(x):
            return {"ok": Path(x).exists(), "path": x, "url": None, "error": None}
        if _is_url(x):
            return {"ok": False, "path": None, "url": x, "error": "no local path returned"}
        return {"ok": False, "path": None, "url": None, "error": f"unexpected item: {x!r}"}

    # Path
    if isinstance(x, Path):
        return {"ok": x.exists(), "path": str(x), "url": None, "error": None}

    return {"ok": False, "path": None, "url": None, "error": f"unexpected type: {type(x).__name__}"}


def _normalize_download_results(items: Iterable[Any]) -> List[Dict[str, Any]]:
    return [_normalize_one_download_result(x) for x in items]


async def _call_download_pdfs(urls: List[str], out_dir: Path, desired_conc: int) -> List[Dict[str, Any]]:
    """
    兼容不同签名的 download_pdfs：
      - 目录参数以“位置参数第2个”传入
      - 并发参数名自动匹配（如无，则不传）
      - 返回值统一规整为标准 dict 结构
    """
    sig = inspect.signature(download_pdfs)
    params = list(sig.parameters.keys())

    conc_kw = None
    for name in ("max_download_concurrency", "max_concurrency", "concurrency", "limit", "max_workers"):
        if name in params:
            conc_kw = name
            break

    if conc_kw:
        raw = await download_pdfs(urls, str(out_dir), **{conc_kw: int(desired_conc)})  # type: ignore[arg-type]
    else:
        raw = await download_pdfs(urls, str(out_dir))  # type: ignore[arg-type]

    return _normalize_download_results(raw)


# ---------------------------
# 主流程
# ---------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Text-only PDF pipeline (download + extract text)")
    p.add_argument("--urls", required=True, help="Path to a text file containing one URL per line")
    p.add_argument("--download-dir", required=True, help="Directory to save downloaded PDFs")
    p.add_argument("--output-dir", required=True, help="Directory to save JSON outputs")
    p.add_argument("--download-concurrency", type=int, default=16, help="Max concurrent downloads")
    p.add_argument("--parse-concurrency", type=int, default=8, help="Max concurrent text parses")
    # 历史别名（可选）
    p.add_argument("--urls-file", dest="urls_file_alias", help=argparse.SUPPRESS)
    p.add_argument("--max-download-concurrency", dest="dl_conc_alias", type=int, help=argparse.SUPPRESS)
    p.add_argument("--max-parse-concurrency", dest="parse_conc_alias", type=int, help=argparse.SUPPRESS)
    return p


async def _run(args: argparse.Namespace) -> None:
    # 解析别名
    if getattr(args, "urls_file_alias", None) and not args.urls:
        args.urls = args.urls_file_alias
    if getattr(args, "dl_conc_alias", None) and not args.download_concurrency:
        args.download_concurrency = args.dl_conc_alias
    if getattr(args, "parse_conc_alias", None) and not args.parse_concurrency:
        args.parse_concurrency = args.parse_conc_alias

    urls_path = Path(args.urls).expanduser().resolve()
    download_dir = Path(args.download_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    _ensure_dir(download_dir)
    _ensure_dir(output_dir)

    # 1) 读取 URL 列表
    urls = _read_urls(urls_path)
    if not urls:
        logger.error("No URLs found in %s", urls_path)
        print(f"[ERROR] No URLs found in {urls_path}")
        return
    logger.info("Loaded %d URLs from %s", len(urls), urls_path)

    # 2) 并发下载（签名自适配 + 返回值鲁棒归一化）
    logger.info("Start downloading to %s (requested concurrency=%d)", download_dir, args.download_concurrency)
    dl_results = await _call_download_pdfs(urls, download_dir, args.download_concurrency)

    ok_items = [r for r in dl_results if r.get("ok") and r.get("path")]
    ok_paths = [Path(r["path"]) for r in ok_items]
    failed = [r for r in dl_results if not r.get("ok")]
    logger.info("Downloaded %d/%d files; %d failed.", len(ok_paths), len(dl_results), len(failed))
    if failed:
        for r in failed[:10]:
            logger.warning("Download failed: %s (%s)", r.get("url") or "", r.get("error") or "")

    if not ok_paths:
        logger.error("No files downloaded successfully. Exiting.")
        print("[ERROR] No files downloaded successfully.")
        return

    # 3) 并发抽取文本（parser.parse_pdfs 内部用线程池）
    logger.info("Start parsing text (concurrency=%d) ...", args.parse_concurrency)
    parse_results = parse_pdfs([str(p) for p in ok_paths], max_parse_concurrency=int(args.parse_concurrency))

    # 4) 写出每个 JSON
    written = 0
    for item in parse_results:
        # 期望 parser 返回：
        # {
        #   "schema_version": "text-only-1.0",
        #   "doc_id": "...",
        #   "pdf_path": "...",
        #   "pages": int | None,
        #   "text_type": "plain",
        #   "extraction": {...},
        #   "text": "..."
        # }
        doc_id = item.get("doc_id") or Path(item.get("pdf_path", "output.pdf")).stem
        out_path = output_dir / f"{doc_id}.json"
        try:
            _write_json(out_path, item)
            written += 1
        except Exception as e:
            logger.exception("Failed to write JSON for %s: %s", doc_id, e)

    logger.info("Done. Wrote %d JSON files to %s", written, output_dir)
    print(f"[OK] Downloaded: {len(ok_paths)}/{len(dl_results)} | Written JSON: {written} | Output: {output_dir}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
