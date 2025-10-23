#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 Google Sheets API Wrapper
التعامل مع Google Sheets
✅ ID دايماً في عمود Z (ثابت)
"""

import logging
from typing import Dict, List, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleSheetsAPI:
    """
    Wrapper للتعامل مع Google Sheets
    """

    # 🎯 تثبيت عمود الـ ID = Z (index 25)
    ID_COLUMN_INDEX = 25  # Z = العمود رقم 26 (0-based = 25)
    ID_COLUMN_LETTER = "Z"

    def __init__(self, credentials_file: str, spreadsheet_id: str, sheet_name: str):
        """
        تهيئة Google Sheets API

        Args:
            credentials_file: مسار ملف credentials.json
            spreadsheet_id: ID الشيت
            sheet_name: اسم الورقة
        """
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name

        # Authentication
        try:
            self.creds = Credentials.from_service_account_file(
                credentials_file,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )

            self.service = build("sheets", "v4", credentials=self.creds)
            self.sheet = self.service.spreadsheets()

            logger.info(f"✅ Google Sheets API initialized: {sheet_name}")
            logger.info(f"🎯 ID column fixed at: {self.ID_COLUMN_LETTER}")

            # التأكد من وجود header في Z1
            self._ensure_id_header()

        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets API: {e}")
            raise

    def _ensure_id_header(self):
        """
        التأكد من وجود header "ID" في العمود Z1
        """
        try:
            # قراءة Z1
            result = (
                self.sheet.values()
                .get(spreadsheetId=self.spreadsheet_id, range=f"{self.sheet_name}!Z1")
                .execute()
            )

            values = result.get("values", [])

            # لو Z1 فاضي أو مش "ID" → نكتب "ID"
            if not values or not values[0] or values[0][0] != "ID":
                logger.info("📝 Setting 'ID' header in column Z1")

                self.sheet.values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.sheet_name}!Z1",
                    valueInputOption="RAW",
                    body={"values": [["ID"]]},
                ).execute()

                logger.info("✅ Header 'ID' added to column Z1")
            else:
                logger.info("✅ Header 'ID' already exists in column Z1")

        except Exception as e:
            logger.warning(f"⚠️ Could not verify/set ID header: {e}")

    def append_emails(self, emails_data: List[Dict]) -> Tuple[bool, str]:
        """
        إضافة Email + ID للشيت

        Email في عمود A
        ID دايماً في عمود Z (ثابت)

        Args:
            emails_data: List of {"email": str, "id": str}

        Returns:
            (success: bool, message: str)
        """
        if not emails_data:
            return True, "No emails to add"

        try:
            # 1️⃣ الحصول على آخر صف في العمود A
            result_range = (
                self.sheet.values()
                .get(spreadsheetId=self.spreadsheet_id, range=f"{self.sheet_name}!A:A")
                .execute()
            )

            existing_values = result_range.get("values", [])
            next_row = len(existing_values) + 1

            # 2️⃣ تجهيز البيانات
            values = []
            for item in emails_data:
                # 🎯 صف من A إلى Z (26 عمود)
                row = [""] * 26

                # Email في A (index 0)
                row[0] = item.get("email", "")

                # ID في Z (index 25)
                item_id = item.get("id", "")

                # ✅ تحقق: ID صالح
                if item_id and item_id not in ["N/A", "pending", "api", ""]:
                    row[self.ID_COLUMN_INDEX] = str(item_id)

                values.append(row)

            body = {"values": values}

            # 3️⃣ تحديد Range (دايماً A:Z)
            last_row = next_row + len(emails_data) - 1
            range_name = f"{self.sheet_name}!A{next_row}:Z{last_row}"

            logger.info(f"📤 Adding {len(emails_data)} rows to range: {range_name}")
            logger.info(f"📧 Email in column A, ID in column Z (fixed)")

            # 4️⃣ إضافة البيانات
            result = (
                self.sheet.values()
                .update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body=body,
                )
                .execute()
            )

            # معلومات عن النتيجة
            updated_rows = result.get("updatedRows", 0)
            updated_range = result.get("updatedRange", "")

            logger.info(f"✅ Added {updated_rows} rows: {updated_range}")

            # عرض عينة من البيانات
            if values:
                sample_email = values[0][0]
                sample_id = values[0][self.ID_COLUMN_INDEX]
                logger.info(f"📝 Sample: Email='{sample_email}', ID@Z='{sample_id}'")

            return True, f"Added {updated_rows} rows"

        except HttpError as e:
            error_details = e.error_details if hasattr(e, "error_details") else str(e)

            # Rate Limit
            if e.resp.status == 429:
                logger.warning("⚠️ Rate limit hit, will retry later")
                return False, "Rate limit"

            # Quota exceeded
            if e.resp.status == 403:
                logger.warning("⚠️ Quota exceeded, will retry later")
                return False, "Quota exceeded"

            logger.error(f"❌ Google Sheets API error: {error_details}")
            return False, str(error_details)

        except Exception as e:
            logger.exception(f"❌ Unexpected error while adding emails: {e}")
            return False, str(e)
