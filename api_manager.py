#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🌐 API Manager
مدير API مع Smart Cache وBurst Mode
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp

from config import (
    CSRF_TOKEN_TTL,
    CACHE_TTL_MIN,
    CACHE_TTL_NORMAL,
    CACHE_TTL_MAX,
    BURST_MODE_DURATION,
    FINAL_STATUSES,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 🧠 Smart Cache Manager
# ═══════════════════════════════════════════════════════════════


class SmartCacheManager:
    """مدير ذاكرة مؤقتة ذكي"""

    def __init__(self):
        self.cache: Optional[List[Dict]] = None
        self.cache_timestamp: Optional[datetime] = None
        self.cache_ttl: float = CACHE_TTL_NORMAL
        self.burst_mode_active: bool = False
        self.burst_mode_started: Optional[datetime] = None
        self.last_changes_count: int = 0
        self.consecutive_quiet_cycles: int = 0
        self.last_successful_cache: Optional[List[Dict]] = None
        self.last_successful_timestamp: Optional[datetime] = None
        self.burst_targets: set = set()

    def is_cache_valid(self) -> bool:
        """تحقق من صلاحية الـ cache"""
        if self.cache is None or self.cache_timestamp is None:
            return False
        if self.burst_mode_active:
            return False
        age = (datetime.now() - self.cache_timestamp).total_seconds()
        return age < self.cache_ttl

    def activate_burst_mode(self, account_id: str):
        """تفعيل وضع Burst"""
        if not self.burst_mode_active:
            self.burst_mode_active = True
            self.burst_mode_started = datetime.now()
            logger.info(f"🚀 BURST MODE ON: {account_id}")
        self.burst_targets.add(account_id)

    def check_burst_mode(self):
        """تحقق وإلغاء وضع Burst إذا انتهى"""
        if not self.burst_mode_active:
            return
        elapsed = (datetime.now() - self.burst_mode_started).total_seconds()
        if elapsed >= BURST_MODE_DURATION:
            self.burst_mode_active = False
            self.burst_mode_started = None
            self.burst_targets.clear()
            logger.info("⚡ BURST MODE OFF")

    def adjust_ttl(self, changes_detected: int):
        """تعديل ذكي لـ TTL"""
        old_ttl = self.cache_ttl

        if changes_detected >= 5:
            self.cache_ttl = CACHE_TTL_MIN
            self.consecutive_quiet_cycles = 0
        elif changes_detected >= 2:
            self.cache_ttl = CACHE_TTL_NORMAL
            self.consecutive_quiet_cycles = 0
        else:
            self.consecutive_quiet_cycles += 1
            if self.consecutive_quiet_cycles >= 3:
                self.cache_ttl = CACHE_TTL_MAX

        if old_ttl != self.cache_ttl:
            logger.info(f"🎯 TTL: {old_ttl:.0f}s → {self.cache_ttl:.0f}s")

    def update_cache(self, new_data: List[Dict], success: bool = True):
        """تحديث الـ cache"""
        if success:
            self.cache = new_data
            self.cache_timestamp = datetime.now()
            self.last_successful_cache = new_data
            self.last_successful_timestamp = datetime.now()
        else:
            logger.warning("⚠️ Using fallback cache")
            if self.last_successful_cache:
                self.cache = self.last_successful_cache
                self.cache_timestamp = self.last_successful_timestamp

    def get_cache(self) -> Optional[List[Dict]]:
        """الحصول على الـ cache"""
        return self.cache

    def get_account_by_id(self, account_id: str) -> Optional[Dict]:
        """البحث بالـ ID"""
        if not self.cache:
            return None
        for account in self.cache:
            if str(account.get("idAccount", "")) == str(account_id):
                return account
        return None

    def get_account_by_email(self, email: str) -> Optional[Dict]:
        """البحث بالإيميل"""
        if not self.cache:
            return None
        email = email.lower().strip()
        for account in self.cache:
            if account.get("Sender", "").lower() == email:
                return account
        return None


# Global smart cache
smart_cache = SmartCacheManager()


# ═══════════════════════════════════════════════════════════════
# 🔐 Optimized API Manager
# ═══════════════════════════════════════════════════════════════


class OptimizedAPIManager:
    """مدير API مع Smart Cache"""

    def __init__(self, config: Dict):
        self.base_url = config["website"]["urls"]["base"]
        self.cookies = config["website"]["cookies"]
        self.defaults = config["website"]["defaults"]
        self.csrf_token = None
        self.csrf_expires_at = None
        self.session = None

    async def initialize(self):
        """تهيئة"""
        await self._ensure_session()
        logger.info("🚀 API Manager initialized")

    async def _ensure_session(self):
        """التأكد من وجود session"""
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                connector=connector, timeout=timeout, cookies=self.cookies
            )

    async def get_csrf_token(self, force_refresh: bool = False) -> Optional[str]:
        """الحصول على CSRF token"""
        if not force_refresh and self.csrf_token and self.csrf_expires_at:
            if datetime.now() < self.csrf_expires_at:
                return self.csrf_token

        logger.info("🔄 Fetching CSRF...")
        await self._ensure_session()

        try:
            async with self.session.get(f"{self.base_url}/senderPage") as resp:
                if resp.status == 200:
                    html = await resp.text()
                    match = re.search(
                        r'<meta name="csrf-token" content="([^"]+)"', html
                    )
                    if match:
                        self.csrf_token = match.group(1)
                        self.csrf_expires_at = datetime.now() + timedelta(
                            seconds=CSRF_TOKEN_TTL
                        )
                        logger.info(f"✅ CSRF cached ({CSRF_TOKEN_TTL}s)")
                        return self.csrf_token
        except Exception as e:
            logger.error(f"❌ CSRF error: {e}")

        return None

    async def fetch_all_accounts_batch(self, force_refresh: bool = False) -> List[Dict]:
        """جلب كل الحسابات"""
        if not force_refresh and smart_cache.is_cache_valid():
            cached = smart_cache.get_cache()
            if cached:
                return cached

        logger.info("🔄 Batch fetch...")

        csrf = await self.get_csrf_token()
        if not csrf:
            smart_cache.update_cache([], success=False)
            return smart_cache.get_cache() or []

        await self._ensure_session()

        try:
            payload = {"date": "0", "bigUpdate": "0", "csrf_token": csrf}

            async with self.session.post(
                f"{self.base_url}/dataFunctions/updateSenderPage", data=payload
            ) as resp:

                if resp.status == 200:
                    data = await resp.json()

                    if "data" in data:
                        accounts = data["data"]

                        INDEX_MAP = {
                            "idAccount": 0,
                            "image": 1,
                            "Sender": 2,
                            "Start": 3,
                            "Last Update": 4,
                            "Taken": 5,
                            "Status": 6,
                            "Available": 7,
                            "password": 8,
                            "backupCodes": 9,
                            "Group": 10,
                            "groupNameId": 11,
                            "Take": 12,
                            "Keep": 13,
                        }

                        parsed = []
                        for account in accounts:
                            if len(account) > INDEX_MAP["Sender"]:
                                acc = {}
                                for key, idx in INDEX_MAP.items():
                                    acc[key] = (
                                        str(account[idx])
                                        if idx < len(account) and account[idx]
                                        else ""
                                    )
                                parsed.append(acc)

                        smart_cache.update_cache(parsed, success=True)
                        logger.info(f"✅ Fetched {len(parsed)} accounts")
                        return parsed

                elif resp.status in [403, 419]:
                    self.csrf_token = None
                    return await self.fetch_all_accounts_batch(force_refresh=True)

        except Exception as e:
            logger.error(f"❌ Fetch error: {e}")
            smart_cache.update_cache([], success=False)

        return smart_cache.get_cache() or []

    async def search_sender_by_id(self, account_id: str) -> Optional[Dict]:
        """البحث بالـ ID"""
        if not smart_cache.is_cache_valid():
            await self.fetch_all_accounts_batch()
        return smart_cache.get_account_by_id(account_id)

    async def search_sender_by_email(self, email: str) -> Optional[Dict]:
        """البحث بالإيميل"""
        if not smart_cache.is_cache_valid():
            await self.fetch_all_accounts_batch()
        return smart_cache.get_account_by_email(email)

    async def add_sender(
        self,
        email: str,
        password: str,
        backup_codes: str = "",
        amount_take: str = "",
        amount_keep: str = "",
    ) -> Tuple[bool, str]:
        """إضافة حساب"""
        csrf = await self.get_csrf_token()
        if not csrf:
            return False, "No CSRF"

        await self._ensure_session()

        payload = {
            "email": email,
            "password": password,
            "backupCodes": backup_codes,
            "groupName": self.defaults["group_name"],
            "accountLock": self.defaults["account_lock"],
            "amountToTake": amount_take or self.defaults.get("amount_take", ""),
            "amountToKeep": amount_keep or self.defaults.get("amount_keep", ""),
            "priority": self.defaults.get("priority", ""),
            "forceProxy": self.defaults.get("force_proxy", ""),
            "userPrice": self.defaults.get("user_price", ""),
            "csrf_token": csrf,
        }

        try:
            async with self.session.post(
                f"{self.base_url}/dataFunctions/addAccount", json=payload
            ) as resp:

                if resp.status == 200:
                    try:
                        data = await resp.json()
                        if "success" in data:
                            smart_cache.cache = None
                            smart_cache.cache_timestamp = None
                            return True, data.get("success", "Success")
                        elif "error" in data:
                            error = data.get("error", "")
                            if "already" in error.lower():
                                return True, "Exists"
                            return False, error
                    except:
                        text = await resp.text()
                        if "success" in text.lower():
                            smart_cache.cache = None
                            smart_cache.cache_timestamp = None
                            return True, "Success"
                        return False, text[:100]

                elif resp.status in [403, 419]:
                    self.csrf_token = None
                    return False, "CSRF expired"

                return False, f"Status {resp.status}"

        except Exception as e:
            return False, str(e)

    async def close(self):
        """إغلاق"""
        if self.session and not self.session.closed:
            await self.session.close()
