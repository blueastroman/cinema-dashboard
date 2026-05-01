ALAMO_ALGOLIA_HEADERS = {
    "X-Algolia-Application-Id": "J21VYKWY3K",
    "X-Algolia-API-Key": "b475e661e58e2a407860db2f4f8f7cff",
    "Content-Type": "application/json",
}

ALAMO_ALGOLIA_QUERY_URL = "https://J21VYKWY3K-dsn.algolia.net/1/indexes/prod_on-sale-presentation/query"


def alamo_presentation_url(market_slug: str, slug: str) -> str:
    return f"https://drafthouse.com/s/mother/v2/schedule/presentation/{market_slug}/{slug}"
