#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🧠 Core Functions & Utilities
الدوال الأساسية ومدراء النظام
"""

import asyncio
import json
import logging
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from config import (
    MONITORED_ACCOUNTS_FILE,
    STATS_FILE,
    POLLING_INTERVALS,
    TRANSITIONAL_STATUSES,
    FINAL_STATUSES,
    STATUS_EMOJIS,
    STATUS_DESCRIPTIONS_AR,
    BURST_MODE_INTERVAL,
)
from api_manager import smart_cache

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# 📊 Statistics
# ═══════════════════════════════════════════════════════════════


@dataclass
class RequestStats:
    """إحصائيات النظام"""

    total_requests: int = 0
    csrf_refreshes: int = 0
    batch_fetches: int = 0
    cache_hits: int = 0
    errors: int = 0
    fast_detections: int = 0
    burst_activations: int = 0
    adaptive_adjustments: int = 0
    last_reset: str = datetime.now().isoformat()

    def save(self):
        try:
            with open(STATS_FILE, "w") as f:
                json.dump(asdict(self), f, indent=2)
        except Exception as e:
            logger.error(f"❌ Save stats error: {e}")

    @classmethod
    def load(cls):
        if Path(STATS_FILE).exists():
            try:
                with open(STATS_FILE, "r") as f:
                    return cls(**json.load(f))
            except:
                pass
        return cls()


stats = RequestStats.load()


# ═══════════════════════════════════════════════════════════════
# 💾 Database Functions
# ═══════════════════════════════════════════════════════════════


def load_monitored_accounts() -> Dict:
    """تحميل الحسابات المراقبة"""
    if Path(MONITORED_ACCOUNTS_FILE).exists():
        try:
            with open(MONITORED_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {}


def save_monitored_accounts(accounts: Dict):
    """حفظ الحسابات"""
    try:
        with open(MONITORED_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"❌ Save error: {e}")


def add_monitored_account(email: str, account_id: str, status: str, chat_id: int):
    """إضافة حساب للمراقبة"""
    accounts = load_monitored_accounts()
    key = f"{account_id}_{email}"
    accounts[key] = {
        "email": email,
        "account_id": account_id,
        "last_known_status": status,
        "chat_id": chat_id,
        "added_at": datetime.now().isoformat(),
        "last_check": datetime.now().isoformat(),
    }
    save_monitored_accounts(accounts)
    logger.info(f"✅ Added: {email} (ID: {account_id})")


def update_monitored_account_status(account_id: str, new_status: str):
    """تحديث حالة حساب"""
    accounts = load_monitored_accounts()
    for key, data in accounts.items():
        if data.get("account_id") == account_id:
            data["last_known_status"] = new_status
            data["last_check"] = datetime.now().isoformat()
            save_monitored_accounts(accounts)
            return
    logger.warning(f"⚠️ ID {account_id} not found")


# ═══════════════════════════════════════════════════════════════
# 🛡️ Helper Functions
# ═══════════════════════════════════════════════════════════════


def is_admin(user_id: int, admin_ids: list) -> bool:
    """التحقق من صلاحيات الأدمن"""
    return not admin_ids or user_id in admin_ids


def get_adaptive_interval(status: str) -> float:
    """الحصول على فاصل زمني ذكي"""
    interval_range = POLLING_INTERVALS.get(status.upper(), POLLING_INTERVALS["DEFAULT"])
    return round(random.uniform(*interval_range), 2)


def format_number(value) -> str:
    """تنسيق الأرقام"""
    if value is None or value == "" or value == "null":
        return "0"
    try:
        value_str = str(value).strip()
        if not value_str.replace(".", "", 1).replace("-", "", 1).isdigit():
            return value_str
        num = float(value_str)
        if abs(num) < 1000:
            return str(int(num)) if num == int(num) else str(num)
        k_value = num / 1000
        if abs(k_value) >= 1000:
            return f"{k_value:,.0f}k"
        else:
            return f"{int(k_value)}k"
    except:
        return str(value)


def get_status_emoji(status: str) -> str:
    """الحصول على emoji"""
    return STATUS_EMOJIS.get(status.upper(), "📊")


def get_status_description_ar(status: str) -> str:
    """الحصول على الوصف العربي"""
    return STATUS_DESCRIPTIONS_AR.get(status.upper(), status)


def parse_sender_data(text: str) -> Dict:
    """تحليل بيانات السيندر"""
    lines = text.strip().split("\n")
    data = {
        "email": "",
        "password": "",
        "codes": [],
        "amount_take": "",
        "amount_keep": "",
    }
    email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if re.match(email_pattern, line):
            data["email"] = line.lower()
        elif "اسحب" in line:
            match = re.search(r"اسحب\s*(\d+)", line)
            if match:
                data["amount_take"] = match.group(1)
        elif "يسيب" in line:
            match = re.search(r"يسيب\s*(\d+)", line)
            if match:
                data["amount_keep"] = match.group(1)
        elif re.match(r"^[\d.]+$", line):
            clean_code = line.split(".")[-1] if "." in line else line
            data["codes"].append(clean_code)
        elif data["email"] and not data["password"]:
            data["password"] = line

    data["codes"] = ",".join(data["codes"])
    return data


# ═══════════════════════════════════════════════════════════════
# 🚀 Monitoring Functions
# ═══════════════════════════════════════════════════════════════


async def wait_for_status_change(
    api_manager, email: str, message_obj, chat_id: int
) -> Tuple[bool, Optional[Dict]]:
    """مراقبة مع Burst Mode"""

    global stats

    await asyncio.sleep(3.0)

    start_time = datetime.now()
    total_elapsed = 0
    last_status = None
    status_changes = []
    account_id = None

    logger.info(f"🔍 Looking for: {email}")

    # البحث الأولي
    for initial_attempt in range(1, 15):
        account_info = await api_manager.search_sender_by_email(email)

        if account_info:
            account_id = account_info.get("idAccount")
            if account_id:
                logger.info(f"✅ Found: {email} (ID: {account_id})")
                smart_cache.activate_burst_mode(account_id)
                break

        await message_obj.edit_text(
            f"🔍 *البحث الأولي*\n\n"
            f"📧 `{email}`\n"
            f"🔄 {initial_attempt}/15\n"
            f"⏱️ ~{total_elapsed:.0f}s",
            parse_mode="Markdown",
        )

        interval = 3.0
        total_elapsed += interval
        await asyncio.sleep(interval)

    if not account_id:
        return False, None

    # المراقبة السريعة
    logger.info(f"🚀 Burst monitoring: {email}")

    max_attempts = 40

    for attempt in range(1, max_attempts + 1):
        try:
            smart_cache.check_burst_mode()

            mode = "🚀 BURST" if smart_cache.burst_mode_active else "🔄 NORMAL"

            account_info = await api_manager.search_sender_by_id(account_id)

            if not account_info:
                logger.warning(f"⚠️ ID {account_id} disappeared!")
                await asyncio.sleep(2.0)
                continue

            status = account_info.get("Status", "غير محدد").upper()

            if status != last_status:
                change_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"📊 {email}: {status} ({change_time:.1f}s)")

                status_changes.append({"status": status, "elapsed": total_elapsed})

                if last_status and status in FINAL_STATUSES:
                    stats.fast_detections += 1

                last_status = status

            is_final = status in FINAL_STATUSES
            is_transitional = status in TRANSITIONAL_STATUSES

            status_ar = get_status_description_ar(status)
            status_type = (
                "✅ نهائية"
                if is_final
                else "⏳ انتقالية" if is_transitional else "❓ غير محددة"
            )

            changes_text = ""
            if len(status_changes) > 1:
                changes_text = "\n📝 *التغييرات:*\n"
                for i, change in enumerate(status_changes[-3:]):
                    changes_text += (
                        f"   {i+1}. `{change['status']}` ({change['elapsed']:.0f}s)\n"
                    )

            await message_obj.edit_text(
                f"{mode} *مراقبة ذكية*\n\n"
                f"📧 `{email}`\n"
                f"🆔 ID: `{account_id}`\n\n"
                f"📊 `{status}`\n"
                f"   {get_status_emoji(status)} {status_ar}\n\n"
                f"🎯 {status_type}\n"
                f"{changes_text}\n"
                f"⏱️ {int(total_elapsed)}s\n"
                f"🔍 {attempt}/{max_attempts}",
                parse_mode="Markdown",
            )

            if is_final:
                logger.info(f"✅ {email} STABLE: {status}")

                if status in ["AVAILABLE", "ACTIVE", "LOGGED", "LOGGED IN"]:
                    add_monitored_account(email, account_id, status, chat_id)

                smart_cache.burst_mode_active = False
                smart_cache.burst_targets.discard(account_id)

                return True, account_info

            interval = BURST_MODE_INTERVAL if smart_cache.burst_mode_active else 4.0

            total_elapsed += interval
            await asyncio.sleep(interval)

        except Exception as e:
            logger.exception(f"❌ Error #{attempt}: {e}")
            await asyncio.sleep(2.0)
            total_elapsed += 2.0

    logger.warning(f"⏱️ {email}: Timeout")

    smart_cache.burst_mode_active = False
    if account_id:
        smart_cache.burst_targets.discard(account_id)

    if account_info:
        status = account_info.get("Status", "").upper()
        if status in ["AVAILABLE", "ACTIVE", "LOGGED", "LOGGED IN"]:
            add_monitored_account(email, account_id, status, chat_id)
        return True, account_info

    return False, None


async def continuous_monitor(api_manager, telegram_bot):
    """المراقب المستمر"""

    logger.info("🔄 Background monitor started")

    while True:
        try:
            accounts = load_monitored_accounts()

            if not accounts:
                await asyncio.sleep(30)
                continue

            all_accounts = await api_manager.fetch_all_accounts_batch()

            accounts_by_id = {
                acc.get("idAccount"): acc
                for acc in all_accounts
                if acc.get("idAccount")
            }

            changes_detected = 0

            for key, data in list(accounts.items()):
                try:
                    account_id = data.get("account_id")
                    if not account_id:
                        continue

                    account_info = accounts_by_id.get(account_id)

                    if not account_info:
                        continue

                    current_status = account_info.get("Status", "").upper()
                    last_status = data["last_known_status"].upper()

                    if current_status != last_status:
                        changes_detected += 1
                        email = data.get("email", "unknown")

                        logger.info(f"🔔 {email}: {last_status} → {current_status}")

                        update_monitored_account_status(account_id, current_status)

                        # إرسال إشعار
                        await send_notification(
                            telegram_bot,
                            email,
                            account_id,
                            last_status,
                            current_status,
                            data["chat_id"],
                            account_info,
                        )
                    else:
                        update_monitored_account_status(account_id, current_status)

                except Exception as e:
                    logger.exception("❌ Check error")

            smart_cache.adjust_ttl(changes_detected)

            statuses = [d["last_known_status"] for d in accounts.values()]

            if "LOGGING" in statuses:
                cycle_delay = random.uniform(10, 20)
            elif "AVAILABLE" in statuses or "ACTIVE" in statuses:
                cycle_delay = random.uniform(30, 60)
            else:
                cycle_delay = random.uniform(60, 120)

            await asyncio.sleep(cycle_delay)

        except Exception as e:
            logger.exception("❌ Monitor error")
            await asyncio.sleep(30)


async def send_notification(
    telegram_bot,
    email: str,
    account_id: str,
    old_status: str,
    new_status: str,
    chat_id: int,
    account_data: Dict,
):
    """إرسال إشعار"""
    try:
        old_emoji = get_status_emoji(old_status)
        new_emoji = get_status_emoji(new_status)
        old_ar = get_status_description_ar(old_status)
        new_ar = get_status_description_ar(new_status)

        notification = (
            f"🔔 *تنبيه تغيير!*\n\n"
            f"📧 `{email}`\n"
            f"🆔 ID: `{account_id}`\n\n"
            f"📊 *السابقة:* `{old_status}`\n"
            f"   {old_emoji} {old_ar}\n\n"
            f"📊 *الجديدة:* `{new_status}`\n"
            f"   {new_emoji} {new_ar}\n\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
        )

        available = format_number(account_data.get("Available", "0"))
        taken = format_number(account_data.get("Taken", "0"))

        if available != "0" or taken != "0":
            notification += f"\n💵 متاح: {available}\n✅ مسحوب: {taken}\n"

        notification += f"\n💡 `/search {email}`"

        await telegram_bot.send_message(
            chat_id=chat_id, text=notification, parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"❌ Notification error: {e}")
