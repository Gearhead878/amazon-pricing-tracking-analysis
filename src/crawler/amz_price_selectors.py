PAGE_READY_XPATH = "//div[@id='aod-pinned-offer']"

PRODUCT_TITLE_XPATH = "//span[@id='productTitle']/text()"

PINNED_OFFER_SELLER_LINK_XPATH = (
    "//div[@id='aod-pinned-offer']//div[@id='aod-offer-soldBy']//a/text()"
)

PINNED_OFFER_AMAZON_SELLER_TEXT_XPATH = (
    "//div[@id='aod-pinned-offer']//div[@id='aod-offer-soldBy']"
    "//span[contains(@class, 'a-size-small') and contains(@class, 'a-color-base')]/text()"
)

PINNED_OFFER_DELIVERY_TIME_XPATH = (
    '//div[@id="pinned-offer-top-id"]//div[@id="mir-layout-DELIVERY_BLOCK"]'
    '//div[not(contains(@id, "HOLIDAY"))]//text()'
)

PINNED_OFFER_PRICE_WHOLE_XPATH = (
    "//div[@id='pinned-offer-top-id']//span[@class='a-price-whole']/text()"
)

PINNED_OFFER_PRICE_FRACTION_XPATH = (
    "//div[@id='pinned-offer-top-id']//span[@class='a-price-fraction']/text()"
)

BOTTOM_OFFERS_XPATH = "//div[@id='aod-offer-list']//div[@id='aod-offer']"

BOTTOM_OFFER_PRICE_WHOLE_XPATH = (
    ".//div[@id='aod-offer-price']//span[@class='a-price-whole']/text()"
)

BOTTOM_OFFER_PRICE_FRACTION_XPATH = (
    ".//div[@id='aod-offer-price']//span[@class='a-price-fraction']/text()"
)

BOTTOM_OFFER_SELLER_LINK_XPATH = ".//div[@id='aod-offer-soldBy']//a/text()"

BOTTOM_OFFER_AMAZON_SELLER_TEXT_XPATH = (
    ".//span[contains(@class, 'a-size-small') and contains(@class, 'a-color-base')]/text()"
)

BOTTOM_OFFER_DELIVERY_TIME_XPATH = (
    './/div[@id="mir-layout-DELIVERY_BLOCK"]//div[not(contains(@id, "HOLIDAY"))]//text()'
)