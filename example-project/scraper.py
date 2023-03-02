"""
This is a small example scraper for web scraping using scrapfly.io Python SDK:

https://scrapfly.io/docs/sdk/python
used in youtube video https://www.youtube.com/watch?v=2fmCcfAb4k4
"""
import csv
import asyncio
from typing import List, Dict, AsyncGenerator
# note:Async Generators allow to pause asynchronous functions that generate results
# alows to use "async for result in async_generator():" syntax

from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse

scrapfly = ScrapflyClient(key="YOUR SCRAPFLY KEY", max_concurrency=3)

async def scrape_businesses(urls: List[str]) -> AsyncGenerator[Dict, None]:
    """scrape yelp.com business pages"""
    to_scrape = [
        ScrapeConfig(url=url, country="US", asp=True, cache=True) 
        for url in urls
    ]
    async for result in scrapfly.concurrent_scrape(to_scrape):
        yield parse_business(result)


def parse_business(result: ScrapeApiResponse) -> Dict:
    """extract business information from yelp.com business page"""
    xpath = result.selector.xpath
    css = result.selector.css
    parsed = {
        "url": result.context["url"], 
        # we can use CSS selectors
        "name": css("h1::text").get(),
        # and for more complex queries XPath
        "phone": xpath('//p[contains(.,"Phone number")]/following-sibling::p/text()').get(),
        "website": xpath('//p[contains(.,"Business website")]/following-sibling::p//text()').get(),
        "address": xpath('//p[contains(.,"Get Directions")]/following-sibling::p//text()').get(),
    }
    return parsed


async def example_scrape_businesses():
    """example run for yelp business scraping"""
    urls = [
        "https://www.yelp.com/biz/capri-laguna-laguna-beach",
        "https://www.yelp.com/biz/sunset-cove-villas-laguna-beach",
        "https://www.yelp.com/biz/knotts-berry-farm-buena-park",
    ]
    async for result in scrape_businesses(urls):
        with open("businesses.csv", "a") as f:
            csv.writer(f).writerow(result.values())


async def scrape_search(url: str, max_pages: int=24) -> AsyncGenerator[List[Dict], None]:
    """Scrape yelp.com search results (all pages)"""
    # 1. get first page
    # 2. parse first page for total amount of pages
    # 3. get other pages concurrently and parse them

    # first page:
    first_page = await scrapfly.async_scrape(ScrapeConfig(
        url=url,
        country="US",
        asp=True,
        cache=True,
    ))
    yield parse_search(first_page)

    # other pages, scrape concurrently:
    total_pages = first_page.selector.css("div[role=navigation]").re("of (\d+)")[0]
    total_pages = int(total_pages)
    if total_pages > max_pages:
        total_pages = max_pages

    other_pages = [
        ScrapeConfig(
            url=url + f"&start={page * 10}",
            asp=True,
            cache=True,
            country="US",
        )
        for page in range(1, total_pages)
    ]
    async for result in scrapfly.concurrent_scrape(other_pages):
        yield parse_search(result)


def parse_search(result: ScrapeApiResponse) -> List[Dict]:
    """parse yelp search for result data"""
    results = []
    for box in result.selector.css("[data-testid=serp-ia-card]"):
        url = box.css("h3 a::attr(href)").get()
        if '/biz/' not in url:  # some are ads - skip
            continue 
        results.append({
            "url": url,
            "name": box.css("h3 a::text").get(),
            "rating": box.css('[aria-label*="star rating"]::attr(aria-label)').get(),
        })
    return results
    

async def example_scrape_search():
    """example run for yelp search scraping"""
    url = "https://www.yelp.com/search?find_desc=tacosl&find_loc=Toronto%2C+ON" 
    async for result in scrape_search(url):
        with open("search.csv", "a") as f:
            writer = csv.writer(f)
            for biz in result:
                writer.writerow(biz.values())


if __name__ == "__main__":
    # example runs:
    asyncio.run(example_scrape_search())
    asyncio.run(example_scrape_businesses())