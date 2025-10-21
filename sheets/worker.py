#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚙️ Google Sheets Worker
Background worker مع 2 timers منفصلة (pending و retry)
بدون حد لعدد الإيميلات - كل الـ batch يروح دفعة واحدة
"""

import asyncio
import logging
import random
from typing import Dict

from .google_api import GoogleSheetsAPI
from .queue_manager import (
    get_pending_batch,
    get_retry_batch,
    clear_batch,
    move_to_retry,
    move_to_failed,
    save_queue
)
from .logger import WeeklyLogger

logger = logging.getLogger(__name__)


async def pending_worker(config: Dict, sheets_api: GoogleSheetsAPI, weekly_log: WeeklyLogger):
    """
    Timer 1: معالجة pending.json (1-10 ثواني)
    
    - يجيب كل الإيميلات من pending.json
    - يحاول يضيفهم كلهم دفعة واحدة للشيت
    - لو نجح: يمسح الملف
    - لو فشل: ينقل كل واحد لـ retry (ما عدا اللي وصلوا 50 محاولة → failed)
    """
    queue_config = config.get("queue", {})
    min_interval = queue_config.get("pending_interval_min", 1)
    max_interval = queue_config.get("pending_interval_max", 10)
    max_retries = queue_config.get("max_retries", 50)
    
    logger.info(f"🔄 Pending worker started (interval: {min_interval}-{max_interval}s)")
    
    while True:
        try:
            # الحصول على كل البيانات (بدون حد)
            batch = get_pending_batch()
            
            if batch:
                emails = [item["email"] for item in batch]
                
                logger.info(f"📤 Processing {len(emails)} emails from pending queue")
                
                # محاولة الإضافة للشيت (كل الـ batch دفعة واحدة)
                success, message = sheets_api.append_emails(emails)
                
                if success:
                    # نجاح: مسح من pending
                    clear_batch("pending.json", emails)
                    
                    # Log
                    log_msg = f"✅ Added {len(emails)} emails to Sheet"
                    logger.info(log_msg)
                    weekly_log.write(log_msg)
                    
                else:
                    # فشل: نقل لـ retry
                    logger.warning(f"⚠️ Failed to add emails: {message}")
                    
                    for item in batch:
                        attempts = item.get("attempts", 0)
                        
                        if attempts < max_retries:
                            move_to_retry(item)
                        else:
                            # وصل للحد الأقصى
                            move_to_failed(item)
                            log_msg = f"❌ {item['email']} moved to failed (max retries: {max_retries})"
                            logger.warning(log_msg)
                            weekly_log.write(log_msg)
                    
                    # مسح من pending
                    clear_batch("pending.json", emails)
            
            # انتظار (1-10 ثواني)
            interval = random.uniform(min_interval, max_interval)
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.exception(f"❌ Error in pending worker: {e}")
            await asyncio.sleep(30)


async def retry_worker(config: Dict, sheets_api: GoogleSheetsAPI, weekly_log: WeeklyLogger):
    """
    Timer 2: معالجة retry.json (30-60 ثانية)
    
    - يجيب كل الإيميلات من retry.json
    - يحاول يضيفهم كلهم دفعة واحدة للشيت
    - لو نجح: يمسح الملف
    - لو فشل: يزيد عداد المحاولات أو ينقل لـ failed
    """
    queue_config = config.get("queue", {})
    min_interval = queue_config.get("retry_interval_min", 30)
    max_interval = queue_config.get("retry_interval_max", 60)
    max_retries = queue_config.get("max_retries", 50)
    
    logger.info(f"🔄 Retry worker started (interval: {min_interval}-{max_interval}s)")
    
    while True:
        try:
            # الحصول على كل البيانات (بدون حد)
            batch = get_retry_batch()
            
            if batch:
                emails = [item["email"] for item in batch]
                
                logger.info(f"🔁 Retrying {len(emails)} emails from retry queue")
                
                # محاولة الإضافة للشيت (كل الـ batch دفعة واحدة)
                success, message = sheets_api.append_emails(emails)
                
                if success:
                    # نجاح: مسح من retry
                    clear_batch("retry.json", emails)
                    
                    # Log
                    log_msg = f"✅ Added {len(emails)} emails to Sheet (retry)"
                    logger.info(log_msg)
                    weekly_log.write(log_msg)
                    
                else:
                    # فشل: زيادة المحاولات أو نقل لـ failed
                    logger.warning(f"⚠️ Retry failed: {message}")
                    
                    updated_batch = []
                    failed_emails = []
                    
                    for item in batch:
                        attempts = item.get("attempts", 0) + 1
                        item["attempts"] = attempts
                        
                        if attempts < max_retries:
                            updated_batch.append(item)
                        else:
                            # وصل للحد الأقصى
                            move_to_failed(item)
                            failed_emails.append(item["email"])
                            log_msg = f"❌ {item['email']} moved to failed (max retries: {max_retries})"
                            logger.warning(log_msg)
                            weekly_log.write(log_msg)
                    
                    # حفظ الباقي
                    save_queue("retry.json", {"emails": updated_batch})
                    
                    if failed_emails:
                        log_msg = f"❌ {len(failed_emails)} emails moved to failed"
                        weekly_log.write(log_msg)
            
            # انتظار (30-60 ثانية)
            interval = random.uniform(min_interval, max_interval)
            await asyncio.sleep(interval)
            
        except Exception as e:
            logger.exception(f"❌ Error in retry worker: {e}")
            await asyncio.sleep(60)


async def start_sheet_worker(config: Dict):
    """
    تشغيل الـ Google Sheets Worker
    
    Args:
        config: إعدادات التطبيق
    """
    try:
        # إعداد Google Sheets API
        sheet_config = config.get("google_sheet", {})
        credentials_file = sheet_config.get("credentials_file", "credentials.json")
        spreadsheet_id = sheet_config.get("spreadsheet_id")
        sheet_name = sheet_config.get("sheet_name", "Emails")
        
        if not spreadsheet_id:
            logger.error("❌ Google Sheet ID not configured!")
            return
        
        sheets_api = GoogleSheetsAPI(credentials_file, spreadsheet_id, sheet_name)
        
        # إعداد Weekly Logger
        log_dir = config.get("queue", {}).get("log_dir", "logs")
        weekly_log = WeeklyLogger(log_dir)
        
        # تشغيل الـ 2 workers
        logger.info("🚀 Starting Google Sheets workers...")
        
        await asyncio.gather(
            pending_worker(config, sheets_api, weekly_log),
            retry_worker(config, sheets_api, weekly_log)
        )
        
    except Exception as e:
        logger.exception(f"❌ Fatal error in sheet worker: {e}")
