"""
Data models for TTB COLA items.
"""
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class TTBItem(BaseModel):
    """Pydantic model for TTB COLA item data."""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "ttb_id": "25059001000222",
                "permit_no": "BWN-FL-21062",
                "serial_number": "25S003",
                "completed_date": "03/12/2025",
                "fanciful_name": "PEACH MANGO",
                "brand_name": "SHOTTYS",
                "origin_code": "16",
                "origin_desc": "FLORIDA",
                "class_type": "82",
                "class_type_desc": "TABLE FLAVORED WINE",
                "url": "https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid=25059001000222"
            }
        }
    )

    ttb_id: str = Field(..., description="TTB ID number")
    permit_no: Optional[str] = Field(None, description="Permit number")
    serial_number: Optional[str] = Field(None, description="Serial number")
    completed_date: Optional[str] = Field(None, description="Completion date")
    fanciful_name: Optional[str] = Field(None, description="Fanciful name")
    brand_name: Optional[str] = Field(None, description="Brand name")
    origin_code: Optional[str] = Field(None, description="Origin code")
    origin_desc: Optional[str] = Field(None, description="Origin description")
    class_type: Optional[str] = Field(None, description="Class/Type code")
    class_type_desc: Optional[str] = Field(None, description="Class/Type description")
    url: str = Field(..., description="URL to the item details page")
