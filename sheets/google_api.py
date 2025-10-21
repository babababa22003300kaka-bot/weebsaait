#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 Google Sheets API Wrapper
التعامل مع Google Sheets
"""

import logging
from typing import List, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleSheetsAPI:
    """
    Wrapper للتعامل مع Google Sheets
    """
    
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
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            
            self.service = build("sheets", "v4", credentials=self.creds)
            self.sheet = self.service.spreadsheets()
            
            logger.info(f"✅ Google Sheets API initialized: {sheet_name}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Google Sheets API: {e}")
            raise
    
    def append_emails(self, emails: List[str]) -> Tuple[bool, str]:
        """
        إضافة إيميلات للشيت (بدون حد للعدد - batch كامل)
        
        Args:
            emails: List من الإيميلات
        
        Returns:
            (success: bool, message: str)
        """
        if not emails:
            return True, "No emails to add"
        
        try:
            # تجهيز البيانات
            values = [[email] for email in emails]
            
            body = {
                "values": values
            }
            
            # الإضافة للشيت
            result = self.sheet.values().append(
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.sheet_name}!A:A",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()
            
            # معلومات عن النتيجة
            updates = result.get("updates", {})
            updated_range = updates.get("updatedRange", "")
            updated_rows = updates.get("updatedRows", 0)
            
            logger.info(f"✅ Added {updated_rows} emails to Sheet: {updated_range}")
            
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
