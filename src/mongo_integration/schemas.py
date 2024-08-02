from pydantic import BaseModel, constr
from typing import Dict, Any, List, Optional
from datetime import datetime


class OrderSchema(BaseModel):
    partnerId: str
    orderId: str
    orderPartnerData: Dict[str, Any] = {}


class ItemOfficialImage(BaseModel):
    s10: str
    s200: str
    s400: str
    s600: str
    s1000: str
    s1500: str
    original: str


class IncomingRawItemsSchema(BaseModel):
    itemOfficialImage: ItemOfficialImage
    itemGenieImage: Optional[Any] = None
    itemComposition: List[Any] = []
    itemLining: List[Any] = []
    itemWashingInstructions: Dict[str, Any] = {}
    itemAdditionalImages: List[Any] = []
    itemHeadline: str
    itemBrandSKUArray: List[str]
    lookUpMyId: int
    itemYear: str
    itemBrandID: constr(min_length=24, max_length=24)
    fashionHint: str
    itemColorId: int
    itemAvailableSizes: List[str]
    itemBrand: str
    NameComplete: str
    integratedItem: bool
    pendingStylesReview: List[int]
    genderId: int
    integrationId: int
    outOfStock: bool
    pendingRevisions: List[int]
    partnerData: Dict[str, Any]
    createdAt: datetime
    ProductDescription: str
    ProductName: str
    SkuName: str
    lastPriceInCents: int
    pattern: bool
    stillImage: bool
    hueId: int
    colorId: int
    itemWebImage: ItemOfficialImage
