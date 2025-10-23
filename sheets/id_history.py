#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📜 ID History Manager
تسجيل الـ IDs المضافة للشيت والاحتفاظ بآخر 7 أيام فقط
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

# ثوابت
HISTORY_FILE = Path("data/id_history.json")
RETENTION_DAYS = 7  # الاحتفاظ بآخر 7 أيام فقط


def load_history() -> dict:
    """
    تحميل سجل الـ IDs
    """
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"ids": []}


def save_history(data: dict):
    """
    حفظ سجل الـ IDs
    """
    try:
        HISTORY_FILE.parent.mkdir(exist_ok=True)
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"❌ Error saving history: {e}")


def cleanup_old_entries(data: dict) -> dict:
    """
    حذف الإدخالات القديمة (أكتر من 7 أيام)
    """
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    
    cleaned_ids = []
    removed_count = 0
    
    for entry in data.get("ids", []):
        try:
            added_at = datetime.fromisoformat(entry["added_at"])
            if added_at > cutoff_date:
                cleaned_ids.append(entry)
            else:
                removed_count += 1
        except:
            # احتفظ بالإدخالات اللي مش قادرين نقرأ تاريخها
            cleaned_ids.append(entry)
    
    if removed_count > 0:
        logger.info(f"🧹 Cleaned {removed_count} old entries (older than {RETENTION_DAYS} days)")
    
    return {"ids": cleaned_ids}


def add_ids_to_history(ids: List[str]):
    """
    إضافة IDs جديدة للسجل مع التنظيف التلقائي
    
    Args:
        ids: قائمة بالـ IDs المراد إضافتها
    """
    if not ids:
        return
    
    try:
        # تحميل السجل الحالي
        history = load_history()
        
        # إضافة الـ IDs الجديدة
        now = datetime.now().isoformat()
        for id_value in ids:
            if id_value and id_value not in ["N/A", "", None]:
                history["ids"].append({
                    "id": str(id_value),
                    "added_at": now
                })
        
        # تنظيف القديم
        history = cleanup_old_entries(history)
        
        # حفظ
        save_history(history)
        
        logger.info(f"📜 Added {len(ids)} IDs to history")
        logger.debug(f"📊 Total IDs in history: {len(history['ids'])}")
        
    except Exception as e:
        logger.error(f"❌ Error adding IDs to history: {e}")


def get_history_count() -> int:
    """
    الحصول على عدد الـ IDs في السجل
    
    Returns:
        عدد الـ IDs المسجلة
    """
    try:
        history = load_history()
        return len(history.get("ids", []))
    except:
        return 0
