# -*- coding: utf-8 -*-
"""
pdf_pipeline.parser — TEXT ONLY, HIGH-PERF MULTI-THREAD

职责：只抽取 PDF 纯文本，返回标准 JSON 字典（供 pipeline 写盘）。
特点：
- 文件级并发 + 单文件内按页段并发（每个线程独立打开 PDF，安全高效）
- 使用 PyMuPDF（fitz）page.get_text("raw")，尽量贴近 PDF 原始排版
- 无任何可选接口 / CLI / OCR / 表格 / 清洗等扩展

输出（每个 PDF 对象）示例：
{
  "schema_version": "text-only-1.0",
  "doc_id": "1224559413",
  "pdf_path": "/abs/path/1224559413.PDF",
  "pages": 180,
  "text_type": "plain",
  "extraction": {
    "engine": "pymupdf",
    "mode": "raw",
    "file_workers": 4,
    "page_workers": 8,
    "chunks": [[0, 22], [23, 45], ...]
  },
  "text": "……全文……"
}
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
import concurrent.futures as cf
import fitz  # PyMuPDF


SCHEMA_VERSION = "text-only-1.0"
TEXT_MODE = "raw"  # "raw" 更贴近 PDF 原排版；不要改为 "text" 除非你要更“阅读友好”的换行
PAGE_SEP = "\n\f\n"  # 页间分隔符：换行 + form feed + 换行，便于后续切页


def _page_range_chunks(total_pages: int, target_workers: int) -> List[Tuple[int, int]]:
    """
    将 [0, total_pages-1] 均匀切分为若干闭区间块：(start_idx, end_idx)
    例如：total_pages=10, workers=3 -> [(0,3),(4,6),(7,9)]
    """
    if total_pages <= 0:
        return []
    w = max(1, min(target_workers, total_pages))
    base = total_pages // w
    rem = total_pages % w
    chunks: List[Tuple[int, int]] = []
    start = 0
    for i in range(w):
        size = base + (1 if i < rem else 0)
        end = start + size - 1
        chunks.append((start, end))
        start = end + 1
    return chunks


def _extract_text_chunk(pdf_path: Path, start_page: int, end_page: int) -> str:
    """
    线程函数：独立打开 PDF，提取 [start_page, end_page] 的文本并连接。
    使用 get_text("raw")，尽量保持原排版空白特征。
    """
    doc = fitz.open(str(pdf_path))
    try:
        parts: List[str] = []
        for pno in range(start_page, end_page + 1):
            page = doc.load_page(pno)
            parts.append(page.get_text(TEXT_MODE) or "")
        return (PAGE_SEP).join(parts)
    finally:
        doc.close()


def _extract_text_single_pdf(pdf_path: Path) -> Dict[str, Any]:
    """
    单文件文本抽取：按页段并发提速。
    - 为避免 PyMuPDF 文档对象的线程安全问题：每个线程自开文档实例。
    """
    pdf_path = pdf_path.resolve()
    doc_id = pdf_path.stem

    # 获取页数
    doc = fitz.open(str(pdf_path))
    try:
        total_pages = doc.page_count
    finally:
        doc.close()

    # 根据机器核数与页数决定单文件内的页段并发数（避免过度并发）
    cpu = os.cpu_count() or 4
    page_workers = max(1, min(cpu * 2, total_pages))  # 适度放大，PyMuPDF 释放 GIL，IO+CPU 混合
    chunks = _page_range_chunks(total_pages, page_workers)

    # 并发抽取所有页段
    texts: List[str] = [""] * len(chunks)
    with cf.ThreadPoolExecutor(max_workers=len(chunks)) as tp:
        futs = []
        for i, (s, e) in enumerate(chunks):
            fut = tp.submit(_extract_text_chunk, pdf_path, s, e)
            futs.append((i, fut))
        for i, fut in futs:
            texts[i] = fut.result()

    # 拼接全文
    full_text = (PAGE_SEP).join(texts)

    return {
        "schema_version": SCHEMA_VERSION,
        "doc_id": doc_id,
        "pdf_path": str(pdf_path),
        "pages": total_pages,
        "text_type": "plain",
        "extraction": {
            "engine": "pymupdf",
            "mode": TEXT_MODE,
            "file_workers": None,   # 由上层 parse_pdfs 控制
            "page_workers": len(chunks),
            "chunks": chunks,
        },
        "text": full_text,
    }


def parse_pdfs(pdf_paths: List[str], max_parse_concurrency: int = 8) -> List[Dict[str, Any]]:
    """
    批量文本抽取（文件级并发）。保持极简接口供 pipeline 调用。
    - pdf_paths: PDF 路径列表
    - max_parse_concurrency: 同时处理的文件数（线程池大小）
    """
    paths = [Path(p).expanduser().resolve() for p in pdf_paths if p]
    if not paths:
        return []

    results: List[Dict[str, Any]] = [None] * len(paths)  # type: ignore
    with cf.ThreadPoolExecutor(max_workers=max_parse_concurrency) as pool:
        futs = []
        for i, p in enumerate(paths):
            fut = pool.submit(_extract_text_single_pdf, p)
            futs.append((i, fut))
        for i, fut in futs:
            try:
                results[i] = fut.result()
                # 回填文件级并发数
                results[i]["extraction"]["file_workers"] = max_parse_concurrency
            except Exception as e:
                # 出错也返回占位，避免上游阻塞
                results[i] = {
                    "schema_version": SCHEMA_VERSION,
                    "doc_id": paths[i].stem,
                    "pdf_path": str(paths[i]),
                    "pages": None,
                    "text_type": "plain",
                    "extraction": {
                        "engine": "pymupdf",
                        "mode": TEXT_MODE,
                        "file_workers": max_parse_concurrency,
                        "page_workers": 0,
                        "chunks": [],
                        "error": f"{type(e).__name__}: {e}",
                    },
                    "text": "",
                }
    return results
