#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🧠 Core Functions & Utilities
الدوال الأساسية ومدراء النظام
✅ نسخة كاملة مع الإضافة الفورية للـ pending.json
"""

import asyncio
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from api_manager import smart_cache
from config import (
    BURST_MODE_INTERVAL,
    FINAL_STATUSES,
    MONITORED_ACCOUNTS_FILE,
    POLLING_INTERVALS,
    STATUS_DESCRIPTIONS_AR,
    STATUS_EMOJIS,
    TRANSITIONAL_STATUSES,
)
from stats import stats

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 💾 Database Functions with ID Validation + Source Tracking
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
    """حفظ الحسابات المراقبة"""
    try:
        with open(MONITORED_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"❌ Save error: {e}")


def add_monitored_account(
    email: str,
    account_id: str,
    status: str,
    chat_id: int,
    source: str = "manual",  # 🆕 NEW PARAMETER
):
    """
    🎯 إضافة حساب للمراقبة مع تخزين الـ ID الموثوق + المصدر
    """
    accounts = load_monitored_accounts()

    # استخدام الـ ID كـ key رئيسي (أكثر أماناً من الإيميل)
    key = f"{account_id}_{email}"

    accounts[key] = {
        "email": email,
        "account_id": account_id,
        "last_known_status": status,
        "chat_id": chat_id,
        "source": source,  # 🆕 تتبع المصدر
        "added_at": datetime.now().isoformat(),
        "last_check": datetime.now().isoformat(),
    }
    save_monitored_accounts(accounts)

    source_label = "البوت 🤖" if source == "bot" else "يدوي 👤"
    logger.info(
        f"✅ Account added to monitoring: {email} (ID: {account_id}, Source: {source_label})"
    )


def update_monitored_account_status(account_id: str, new_status: str):
    """
    🎯 تحديث الحالة باستخدام الـ ID
    """
    accounts = load_monitored_accounts()

    # البحث بالـ ID
    for key, data in accounts.items():
        if data.get("account_id") == account_id:
            data["last_known_status"] = new_status
            data["last_check"] = datetime.now().isoformat()
            save_monitored_accounts(accounts)
            return

    logger.warning(f"⚠️ Account ID {account_id} not found in monitoring list")


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
    """الحصول على emoji للحالة"""
    return STATUS_EMOJIS.get(status.upper(), "📊")


def get_status_description_ar(status: str) -> str:
    """الحصول على الوصف العربي للحالة"""
    return STATUS_DESCRIPTIONS_AR.get(status.upper(), status)


def parse_sender_data(text: str) -> Dict:
    """تحليل بيانات السيندر من النص"""
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
# 🆕 Queue Management for Google Sheets (IMMEDIATE ADDITION)
# ═══════════════════════════════════════════════════════════════


def add_to_pending_queue_immediately(email: str, account_id: str):
    """
    🆕 إضافة فورية للإيميل والID في pending.json (بدون انتظار)
    تستخدم عند اكتشاف الـ ID مباشرة
    """
    pending_file = Path("data/pending.json")
    pending_file.parent.mkdir(exist_ok=True)

    # تحميل البيانات الحالية
    if pending_file.exists():
        try:
            with open(pending_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except:
            data = {"emails": []}
    else:
        data = {"emails": []}

    # إضافة الإيميل والـ ID فوراً
    data["emails"].append(
        {"email": email, "id": account_id, "added_at": datetime.now().isoformat()}
    )

    # حفظ
    with open(pending_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"📝 Added {email} (ID: {account_id}) to pending queue IMMEDIATELY")


def add_to_pending_queue(email: str):
    """
    دالة للتوافق مع Web API - تضيف بدون ID
    """
    pending_file = Path("data/pending.json")
    pending_file.parent.mkdir(exist_ok=True)

    # تحميل البيانات الحالية
    if pending_file.exists():
        try:
            with open(pending_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except:
            data = {"emails": []}
    else:
        data = {"emails": []}

    # إضافة الإيميل بدون ID
    data["emails"].append(
        {
            "email": email,
            "id": "N/A",  # سيتم تحديثه لاحقاً
            "added_at": datetime.now().isoformat(),
        }
    )

    # حفظ
    with open(pending_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"📝 Added {email} to pending queue (via API)")


# ═══════════════════════════════════════════════════════════════
# 🚀 Burst Mode Initial Monitoring + Source Tracking
# ═══════════════════════════════════════════════════════════════


async def wait_for_status_change(
    api_manager,
    email: str,
    message_obj,
    chat_id: int,
    default_group_name: str,  # 🆕 NEW PARAMETER
) -> Tuple[bool, Optional[Dict]]:
    """
    🚀 مراقبة مع Burst Mode المؤقت + تحديد المصدر

    عند إضافة حساب جديد:
    1. تفعيل Burst Mode (تحديث cache كل 2.5 ثانية)
    2. 🆕 إضافة فورية لـ pending.json عند اكتشاف ID
    3. مراقبة سريعة جداً للحساب الجديد
    4. إضافة للمراقبة فقط لو: AVAILABLE + جروب مطابق
    """

    global stats

    await asyncio.sleep(3.0)

    start_time = datetime.now()
    total_elapsed = 0
    last_status = None
    status_changes = []
    stable_count = 0
    account_id = None

    # 🚀 الخطوة 1: جلب الحساب لأول مرة والحصول على الـ ID
    logger.info(f"🔍 Looking for new account: {email}")

    for initial_attempt in range(1, 15):  # 15 محاولة = ~45 ثانية
        account_info = await api_manager.search_sender_by_email(email)

        if account_info:
            account_id = account_info.get("idAccount")
            if account_id:
                logger.info(f"✅ Found account: {email} (ID: {account_id})")

                # 🚀 تفعيل Burst Mode لهذا الحساب
                smart_cache.activate_burst_mode(account_id)

                # 🆕 إضافة فورية للـ pending.json (نفس اللحظة)
                add_to_pending_queue_immediately(email, account_id)

                break

        await message_obj.edit_text(
            f"🔍 *البحث الأولي عن الحساب*\n\n"
            f"📧 `{email}`\n"
            f"🔄 المحاولة: {initial_attempt}/15\n"
            f"⏱️ ~{total_elapsed:.0f}s",
            parse_mode="Markdown",
        )

        interval = 3.0
        total_elapsed += interval
        await asyncio.sleep(interval)

    if not account_id:
        return False, None

    # 🚀 الخطوة 2: مراقبة سريعة مع Burst Mode
    logger.info(f"🚀 Starting burst monitoring for {email} (ID: {account_id})")

    max_attempts = 40  # 40 محاولة * 2.5 ثانية = 100 ثانية max

    for attempt in range(1, max_attempts + 1):
        try:
            # تحقق من حالة Burst Mode
            smart_cache.check_burst_mode()

            mode_indicator = (
                "🚀 BURST" if smart_cache.burst_mode_active else "🔄 NORMAL"
            )

            # 🎯 البحث بالـ ID (أكثر أماناً)
            account_info = await api_manager.search_sender_by_id(account_id)

            if not account_info:
                logger.warning(f"⚠️ Account ID {account_id} disappeared!")
                await asyncio.sleep(2.0)
                continue

            status = account_info.get("Status", "غير محدد").upper()

            # تتبع التغييرات
            if status != last_status:
                change_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"📊 {email} status: {status} ({change_time:.1f}s)")

                status_changes.append(
                    {"status": status, "time": datetime.now(), "elapsed": total_elapsed}
                )

                if last_status and status in FINAL_STATUSES:
                    stats.fast_detections += 1
                    logger.info(
                        f"⚡ FAST: {last_status} → {status} in {change_time:.1f}s"
                    )

                last_status = status
                stable_count = 0
            else:
                stable_count += 1

            # تحديد نوع الحالة
            is_final = status in FINAL_STATUSES
            is_transitional = status in TRANSITIONAL_STATUSES

            status_ar = get_status_description_ar(status)
            status_type = (
                "✅ نهائية"
                if is_final
                else "⏳ انتقالية" if is_transitional else "❓ غير محددة"
            )

            # عرض سجل التغييرات
            changes_text = ""
            if len(status_changes) > 1:
                changes_text = "\n📝 *التغييرات:*\n"
                for i, change in enumerate(status_changes[-3:]):
                    changes_text += (
                        f"   {i+1}. `{change['status']}` ({change['elapsed']:.0f}s)\n"
                    )

            # رسالة التحديث
            await message_obj.edit_text(
                f"{mode_indicator} *مراقبة ذكية*\n\n"
                f"📧 `{email}`\n"
                f"🆔 ID: `{account_id}`\n"
                f"📊 *تمت الإضافة لـ Google Sheets*\n\n"
                f"📊 *الحالة:* `{status}`\n"
                f"   {get_status_emoji(status)} {status_ar}\n\n"
                f"🎯 النوع: {status_type}\n"
                f"🔄 الاستقرار: {stable_count}/2\n"
                f"{changes_text}\n"
                f"⏱️ الوقت: {int(total_elapsed)}s\n"
                f"🔍 المحاولة: {attempt}/{max_attempts}",
                parse_mode="Markdown",
            )

            # 🆕 منطق التوقف + شرط الإضافة الجديد
            if is_final:
                response_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"✅ {email} STABLE at {status} in {response_time:.1f}s")

                # 🆕 إضافة للمراقبة فقط لو: AVAILABLE + جروب مطابق
                added_to_monitor = False

                if status == "AVAILABLE":
                    group_name = account_info.get("Group", "")
                    if group_name == default_group_name:  # ← مطابقة دقيقة
                        add_monitored_account(
                            email,
                            account_id,
                            status,
                            chat_id,
                            source="bot",  # 🆕 من البوت
                        )
                        added_to_monitor = True

                # إلغاء Burst Mode
                smart_cache.burst_mode_active = False
                smart_cache.burst_targets.discard(account_id)

                return True, account_info

            # فاصل زمني
            if smart_cache.burst_mode_active:
                interval = BURST_MODE_INTERVAL
            else:
                interval = 4.0 if is_transitional else 5.0

            total_elapsed += interval
            await asyncio.sleep(interval)

        except Exception as e:
            logger.exception(f"❌ Monitoring error #{attempt}: {e}")
            await asyncio.sleep(2.0)
            total_elapsed += 2.0

    # انتهت المحاولات
    logger.warning(f"⏱️ {email}: Timeout, final status: {last_status}")

    # إلغاء Burst Mode
    smart_cache.burst_mode_active = False
    if account_id:
        smart_cache.burst_targets.discard(account_id)

    # 🆕 شرط الإضافة المحدّث
    if account_info:
        status = account_info.get("Status", "").upper()
        if status == "AVAILABLE":
            group_name = account_info.get("Group", "")
            if group_name == default_group_name:
                add_monitored_account(email, account_id, status, chat_id, source="bot")
        return True, account_info

    return False, None


# ═══════════════════════════════════════════════════════════════
# 📧 Notification Function with Source Display
# ═══════════════════════════════════════════════════════════════


async def send_status_notification(
    telegram_bot,
    email: str,
    account_id: str,
    old_status: str,
    new_status: str,
    chat_id: int,
    account_data: Dict,
    source: str = "manual",  # 🆕 NEW PARAMETER
):
    """
    ✅ إرسال إشعار تغيير الحالة مع عرض المصدر
    """
    try:
        # Skip if no valid chat_id
        if not chat_id:
            logger.info(f"ℹ️ Skip notification for {email}: no chat_id")
            return

        old_emoji = get_status_emoji(old_status)
        new_emoji = get_status_emoji(new_status)

        old_status_ar = get_status_description_ar(old_status)
        new_status_ar = get_status_description_ar(new_status)

        # 🆕 Source line
        source_line = "🤖 المصدر: من البوت" if source == "bot" else "👤 المصدر: يدوي"

        notification = (
            f"🔔 *تنبيه تغيير الحالة!*\n\n"
            f"📧 `{email}`\n"
            f"{source_line}\n"  # 🆕 NEW LINE
            f"🆔 ID: `{account_id}`\n\n"
            f"📊 *الحالة السابقة:*\n"
            f"   `{old_status}`\n"
            f"   {old_emoji} {old_status_ar}\n\n"
            f"📊 *الحالة الجديدة:*\n"
            f"   `{new_status}`\n"
            f"   {new_emoji} {new_status_ar}\n\n"
            f"🕐 الوقت: {datetime.now().strftime('%H:%M:%S')}\n"
        )

        available = format_number(account_data.get("Available", "0"))
        taken = format_number(account_data.get("Taken", "0"))

        if available != "0" or taken != "0":
            notification += f"\n💵 المتاح: {available}\n✅ المسحوب: {taken}\n"

        notification += f"\n💡 `/search {email}` للتفاصيل"

        await telegram_bot.send_message(
            chat_id=chat_id, text=notification, parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"❌ Failed to send notification: {e}")


# ═══════════════════════════════════════════════════════════════
# 🔄 Background Monitor with Smart TTL + Auto-Discovery
# ═══════════════════════════════════════════════════════════════


async def continuous_monitor(
    api_manager,
    telegram_bot,
    default_group_name: str,  # 🆕 NEW PARAMETER
    default_chat_id: Optional[int],  # 🆕 NEW PARAMETER
):
    """
    🎯 مراقب مستمر مع Smart TTL + Auto-Discovery
    """

    logger.info("🔄 Background monitor started (Smart TTL + Auto-Discovery)")

    while True:
        try:
            accounts = load_monitored_accounts()

            # Fetch all accounts
            all_accounts = await api_manager.fetch_all_accounts_batch()

            # Build ID dictionary
            accounts_by_id = {
                acc.get("idAccount"): acc
                for acc in all_accounts
                if acc.get("idAccount")
            }

            # 🆕 AUTO-DISCOVERY LOGIC
            existing_ids = {
                data.get("account_id")
                for data in accounts.values()
                if data.get("account_id")
            }

            auto_added = False
            for account in all_accounts:
                account_id = account.get("idAccount")

                # Skip if:
                # - No ID
                # - Already monitored
                # - Not AVAILABLE
                # - Group doesn't match
                if (
                    not account_id
                    or account_id in existing_ids
                    or account.get("Status", "").upper() != "AVAILABLE"
                    or account.get("Group", "") != default_group_name  # 🎯 exact match
                ):
                    continue

                # Auto-add
                email = account.get("Sender", "")
                chat_id = default_chat_id or 0
                add_monitored_account(
                    email,
                    account_id,
                    "AVAILABLE",
                    chat_id,
                    source="manual",  # 🆕 auto-discovered = manual
                )
                existing_ids.add(account_id)
                auto_added = True
                logger.info(f"✅ Auto-monitored {email} (AVAILABLE + default group)")

            # Reload if auto-added
            if auto_added:
                accounts = load_monitored_accounts()

            # Skip if no accounts
            if not accounts:
                await asyncio.sleep(30)
                continue

            changes_detected = 0

            for key, data in list(accounts.items()):
                try:
                    account_id = data.get("account_id")
                    if not account_id:
                        continue

                    # 🎯 البحث بالـ ID (أكثر أماناً من الإيميل)
                    account_info = accounts_by_id.get(account_id)

                    if not account_info:
                        logger.warning(f"⚠️ Account ID {account_id} not found in batch")
                        continue

                    current_status = account_info.get("Status", "غير محدد").upper()
                    last_status = data["last_known_status"].upper()

                    if current_status != last_status:
                        changes_detected += 1
                        email = data.get("email", "unknown")

                        logger.info(f"🔔 {email}: {last_status} → {current_status}")

                        if current_status in ["BACKUP CODE WRONG", "WRONG DETAILS"]:
                            logger.warning(
                                f"⚠️ {email} needs attention: {current_status}"
                            )
                        elif current_status == "TRANSFER LIST IS FULL":
                            logger.info(f"📦 {email} transfer list full")
                        elif current_status == "AMOUNT TAKEN":
                            logger.info(f"💸 {email} amount taken")

                        update_monitored_account_status(account_id, current_status)

                        # ✅ إرسال الإشعار مع المصدر
                        await send_status_notification(
                            telegram_bot,
                            email,
                            account_id,
                            last_status,
                            current_status,
                            data["chat_id"],
                            account_info,
                            data.get("source", "manual"),  # 🆕 pass source
                        )
                    else:
                        update_monitored_account_status(account_id, current_status)

                except Exception as e:
                    logger.exception(f"❌ Error checking account")

            # 🎯 تعديل ذكي للـ TTL بناءً على النشاط
            smart_cache.adjust_ttl(changes_detected)

            # فترة الانتظار
            statuses = [d["last_known_status"] for d in accounts.values()]

            if "LOGGING" in statuses:
                cycle_delay = random.uniform(10, 20)
            elif "AVAILABLE" in statuses or "ACTIVE" in statuses:
                cycle_delay = random.uniform(30, 60)
            else:
                cycle_delay = random.uniform(60, 120)

            logger.debug(
                f"💤 Next check in {cycle_delay:.1f}s (TTL={smart_cache.cache_ttl:.0f}s, changes={changes_detected})"
            )
            await asyncio.sleep(cycle_delay)

        except Exception as e:
            logger.exception("❌ Monitor error")
            await asyncio.sleep(30)
